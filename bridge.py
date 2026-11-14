#!/usr/bin/env python3
"""Tractive -> CalTopo position bridge — many teams, many dogs, one worker.

OFF BY DEFAULT. The switch is each collar's own Tractive LIVE mode:

- Turn LIVE tracking on (in the Tractive app, or `bridge.py --on`) and the
  bridge relays every position fix from that collar to a CalTopo live track.
- Turn LIVE off (app, or `bridge.py --off`) and that dog goes silent.
  No positions are reported outside a deployment.

GROUPS. A group is one team: one Tractive "bridge" account that the team's
handlers share their collars with, one CalTopo group key, and its dogs. One
worker runs every group; each group holds its own Tractive event channel, so
a problem with one team's account never silences another team.

CALL SIGNS. Each deployment posts to a dated device id, so every deployment
is its own CalTopo track and old maps never come back to life:

    {GROUP}-{Dog}-{yymmdd}            legacy (no call-sign secret)
    {GROUP}-{Dog}-{yymmdd}-{tag}      with a call-sign secret (recommended)

The tag is an HMAC of the collar id and the date under the group's secret:
nobody can guess tomorrow's call sign or another team's dog from today's,
but a worker restart mid-search recomputes the same one, so the track never
splits. When a deployment starts the bridge pushes the call sign to the
group's notify URLs (e.g. an ntfy.sh topic) so the handler can paste it into
the incident map.

The worker re-issues LIVE before Tractive's ~300s timeout, so a deployment
never times out mid-search, and losing cell never ends one: out of coverage
the collar reports LIVE-off exactly like an operator stop, so the bridge
only treats LIVE-off as a stop when the collar is demonstrably reachable,
and otherwise keeps re-arming until coverage returns.

Commands:
  bridge.py                run the relay worker (deploy this)
  bridge.py --list         list groups and dogs (incl. collars shared with
                           the bridge account but not yet configured)
  bridge.py --on           start a deployment: enable LIVE, print call sign
  bridge.py --off          end a deployment: disable LIVE
  bridge.py --status       show LIVE state, battery, call sign, last fix
  bridge.py --once         post the latest fix to CalTopo once (test)
  bridge.py --export DATE  export a day's recorded positions as a GPX file
                           (today | yesterday | yymmdd | YYYY-MM-DD).
                           Works for no-coverage days: the collar uploads its
                           offline fixes later. Touches no CalTopo map.
  Select with --group NAME and/or --dog NAME (required when ambiguous).

Configuration: BRIDGE_CONFIG (JSON, or a path to a JSON file) for groups;
without it, the single-dog environment variables still work unchanged. See
.env.example and README.md.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp
from aiotractive import Tractive
from aiotractive.exceptions import TractiveError

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

LOG = logging.getLogger("bridge")

CALTOPO_URL = "https://caltopo.com/api/v1/position/report/{group}"

# One-off mid-day track splits, keyed by date (yymmdd) — legacy single-dog
# mode only. CALTOPO_TRACK_SUFFIX overrides this if set.
TRACK_SUFFIXES = {
    "260809": "b",  # search track split off from the drive-out track
}

# A fix this fresh can only come from the ~4s LIVE clock; default mode is
# ~2-3 min apart. Used both to adopt an in-progress deployment on boot and to
# tell a deliberate LIVE-off from one caused by losing cell.
LIVE_FIX_AGE_SECONDS = 20

TAG_LENGTH = 5
SAFE_NAME = re.compile(r"[^A-Za-z0-9_]+")


# ---- configuration --------------------------------------------------------

@dataclass
class DogConfig:
    name: str                       # human name, used in labels and logs
    device_base: str                # call-sign piece, e.g. "Storm"
    tracker_id: str = ""            # Tractive collar id; "" = the account's only collar
    notify: list[str] = field(default_factory=list)


@dataclass
class GroupConfig:
    name: str
    tractive_email: str
    tractive_password: str
    caltopo_group: str              # first call-sign segment (no hyphens)
    callsign_secret: str = ""       # "" = legacy guessable call signs
    tz: str = "America/Los_Angeles"
    notify: list[str] = field(default_factory=list)
    auto_enroll: bool = True        # relay collars shared but not listed
    dogs: list[DogConfig] = field(default_factory=list)
    legacy: bool = False            # built from single-dog env vars


class ConfigError(Exception):
    pass


def safe_name(name: str) -> str:
    """Call-sign-safe version of a dog name (CalTopo splits on hyphens)."""
    cleaned = SAFE_NAME.sub("", name.replace(" ", "_"))
    return cleaned or "Dog"


def _secret(obj: dict, key: str, where: str, required: bool = True) -> str:
    """Read `key`, or the env var named by `key_env` (keeps secrets out of JSON)."""
    if obj.get(key):
        return str(obj[key])
    env_name = obj.get(f"{key}_env")
    if env_name:
        value = os.environ.get(env_name, "").strip()
        if not value:
            raise ConfigError(f"{where}: environment variable {env_name} is empty")
        return value
    if required:
        raise ConfigError(f"{where}: needs {key} or {key}_env")
    return ""


def parse_config(raw: dict) -> list[GroupConfig]:
    groups_raw = raw.get("groups")
    if not isinstance(groups_raw, list) or not groups_raw:
        raise ConfigError("config needs a non-empty \"groups\" list")
    groups: list[GroupConfig] = []
    seen_groups: set[str] = set()
    for i, g in enumerate(groups_raw):
        name = str(g.get("name") or "").strip()
        if not name:
            raise ConfigError(f"group #{i + 1} needs a name")
        where = f"group {name!r}"
        if name.lower() in seen_groups:
            raise ConfigError(f"duplicate group name {name!r}")
        seen_groups.add(name.lower())
        caltopo_group = _secret(g, "caltopo_group", where)
        if "-" in caltopo_group:
            raise ConfigError(f"{where}: caltopo_group may not contain '-' "
                              "(CalTopo splits the call sign at the first hyphen)")
        group = GroupConfig(
            name=name,
            tractive_email=_secret(g, "tractive_email", where),
            tractive_password=_secret(g, "tractive_password", where),
            caltopo_group=caltopo_group,
            callsign_secret=_secret(g, "callsign_secret", where, required=False),
            tz=str(g.get("tz") or "America/Los_Angeles"),
            notify=list(g.get("notify") or []),
            auto_enroll=bool(g.get("auto_enroll", True)),
        )
        ZoneInfo(group.tz)  # fail fast on a bad zone name
        seen_dogs: set[str] = set()
        for d in g.get("dogs") or []:
            dog_name = str(d.get("name") or "").strip()
            if not dog_name:
                raise ConfigError(f"{where}: every dog needs a name")
            base = safe_name(str(d.get("device_id") or dog_name))
            if base.lower() in seen_dogs:
                raise ConfigError(f"{where}: two dogs map to call-sign name {base!r}")
            seen_dogs.add(base.lower())
            group.dogs.append(DogConfig(
                name=dog_name,
                device_base=base,
                tracker_id=str(d.get("tracker_id") or "").strip(),
                notify=list(d.get("notify") or []),
            ))
        if len(group.dogs) > 1 and any(not d.tracker_id for d in group.dogs):
            raise ConfigError(f"{where}: with more than one dog, each needs tracker_id")
        groups.append(group)
    return groups


def load_config() -> list[GroupConfig]:
    source = os.environ.get("BRIDGE_CONFIG", "").strip()
    if source:
        try:
            if source.startswith("{"):
                raw = json.loads(source)
            else:
                with open(source, encoding="utf-8") as fh:
                    raw = json.load(fh)
        except (OSError, json.JSONDecodeError) as err:
            raise ConfigError(f"BRIDGE_CONFIG unreadable: {err}") from err
        return parse_config(raw)

    # Legacy single-dog mode: the original environment variables.
    def require(name: str) -> str:
        value = os.environ.get(name, "").strip()
        if not value:
            raise ConfigError(f"Missing required environment variable: {name}")
        return value

    caltopo_group = require("CALTOPO_GROUP")
    base = require("CALTOPO_DEVICE_ID")
    return [GroupConfig(
        name=caltopo_group,
        tractive_email=require("TRACTIVE_EMAIL"),
        tractive_password=require("TRACTIVE_PASSWORD"),
        caltopo_group=caltopo_group,
        callsign_secret=os.environ.get("CALLSIGN_SECRET", "").strip(),
        tz=os.environ.get("BRIDGE_TZ", "America/Los_Angeles"),
        notify=[u for u in os.environ.get("NOTIFY_URLS", "").split() if u],
        auto_enroll=False,
        dogs=[DogConfig(name=base, device_base=base,
                        tracker_id=os.environ.get("TRACTIVE_TRACKER_ID", "").strip())],
        legacy=True,
    )]


# ---- call signs -----------------------------------------------------------

def callsign_tag(secret: str, tracker_id: str, day: str) -> str:
    """Unguessable, restart-stable tag for one collar on one day."""
    digest = hmac.new(secret.encode(), f"{tracker_id}|{day}".encode(),
                      hashlib.sha256).digest()
    return base64.b32encode(digest).decode().lower()[:TAG_LENGTH]


# ---- notifications --------------------------------------------------------

async def notify(http: aiohttp.ClientSession, urls: list[str], text: str) -> None:
    """Best-effort push. Plain-text POST suits ntfy.sh; Slack/Discord get JSON."""
    for url in urls:
        try:
            if "hooks.slack.com" in url:
                kwargs = {"json": {"text": text}}
            elif "discord.com/api/webhooks" in url:
                kwargs = {"json": {"content": text}}
            else:
                kwargs = {"data": text.encode(), "headers": {"Title": "K9 deployment"}}
            async with http.post(url, timeout=aiohttp.ClientTimeout(total=10),
                                 **kwargs) as resp:
                if resp.status >= 300:
                    LOG.warning("Notify to %s failed: HTTP %s", _redact(url), resp.status)
        except Exception as err:  # a notifier must never take down a relay
            LOG.warning("Notify to %s failed: %s", _redact(url), err)


def _redact(url: str) -> str:
    return url.split("?")[0][:40] + "…"


# ---- one dog --------------------------------------------------------------

class Dog:
    """One collar's deployment state. All the single-dog logic lives here."""

    def __init__(self, group: GroupBridge, cfg: DogConfig) -> None:
        self.group = group
        self.cfg = cfg
        self.tracker = None
        self.tracker_id = cfg.tracker_id
        # intent_on = a deployment is meant to be running (survives cell loss).
        # live_active = LIVE is actually up right now. Losing cell clears the
        # second, never the first — only an operator stop clears intent.
        self.intent_on = False
        self.live_active = False
        self.lost_since: float | None = None
        self.last_keepalive: float = 0.0
        self.deployment_started: float | None = None
        self.last_fix_time: float | None = None
        self.device_id: str | None = None  # minted per deployment

    @property
    def name(self) -> str:
        return self.cfg.name

    @property
    def label(self) -> str:
        return self.cfg.name if self.group.cfg.legacy else f"{self.group.cfg.name} – {self.cfg.name}"

    def dated_device_id(self) -> str:
        g = self.group.cfg
        day = datetime.now(ZoneInfo(g.tz)).strftime("%y%m%d")
        parts = [self.cfg.device_base, day]
        if g.callsign_secret:
            parts.append(callsign_tag(g.callsign_secret,
                                      self.tracker_id or self.cfg.device_base, day))
        if g.legacy:
            suffix = (os.environ.get("CALTOPO_TRACK_SUFFIX", "").strip()
                      or TRACK_SUFFIXES.get(day, ""))
            if suffix:
                parts.append(suffix)
        return "-".join(parts)

    def call_sign(self, device_id: str | None = None) -> str:
        return f"{self.group.cfg.caltopo_group}-{device_id or self.device_id or '?'}"

    def _start_deployment(self) -> None:
        self.intent_on = True
        self.deployment_started = time.time()
        self.device_id = self.dated_device_id()

    def _end_deployment(self) -> None:
        self.intent_on = False
        self.live_active = False
        self.lost_since = None
        self.deployment_started = None
        self.device_id = None

    async def adopt_running_deployment(self) -> None:
        """Pick up a deployment already running when the worker boots."""
        try:
            report = await self.tracker.pos_report()
        except Exception:
            LOG.exception("[%s] Could not check for a running deployment on boot", self.label)
            return
        fix_time = report.get("time")
        if not fix_time:
            return
        age = time.time() - fix_time
        if age <= LIVE_FIX_AGE_SECONDS:
            self._start_deployment()
            self.live_active = True
            LOG.info("[%s] Adopted a deployment already in progress (fix %.0fs old) "
                     "— call sign %s", self.label, age, self.call_sign())

    async def handle_event(self, event: dict, http: aiohttp.ClientSession) -> None:
        live = event.get("live_tracking")
        if live is not None:
            on = bool(live.get("active") or live.get("pending"))
            if on:
                if not self.intent_on:
                    self._start_deployment()
                    LOG.info("[%s] DEPLOYMENT ON — call sign %s", self.label, self.call_sign())
                    await notify(http, self.cfg.notify + self.group.cfg.notify,
                                 self.deployment_message())
                elif not self.live_active:
                    offline = time.time() - (self.lost_since or time.time())
                    LOG.info("[%s] LIVE RE-ARMED after %.0fs offline — same track %s",
                             self.label, offline, self.call_sign())
                self.live_active = True
                self.lost_since = None
            elif self.intent_on and self.live_active:
                # Losing cell and being switched off both arrive as LIVE-off.
                # Treat it as a stop only when the collar is demonstrably still
                # reachable; otherwise stay armed and keep re-arming.
                stale = (self.last_fix_time is None
                         or time.time() - self.last_fix_time > LIVE_FIX_AGE_SECONDS)
                if live.get("reconnecting") or stale:
                    self.live_active = False
                    self.lost_since = time.time()
                    LOG.warning(
                        "[%s] LIVE LOST (collar unreachable — no cell?) — track %s "
                        "stays armed, re-arming every %ss for up to %.0f min",
                        self.label, self.call_sign(), self.group.rearm_seconds,
                        self.group.rearm_max_seconds / 60)
                else:
                    LOG.info("[%s] DEPLOYMENT OFF — operator stop, feed silent "
                             "(track %s is done)", self.label, self.call_sign())
                    self._end_deployment()

        position = event.get("position")
        # Post whatever arrives while armed: during a re-arm the collar reports
        # at the coarse default interval, and a sparse track beats a dead one.
        if position and self.intent_on:
            await self.post(position, http)

    def deployment_message(self) -> str:
        return (f"{self.label} deployed. CalTopo call sign: {self.call_sign()}\n"
                "Add it to the incident map now (Live track → Fleet/Email/Other); "
                "tracks do not backfill.")

    async def post(self, position: dict, http: aiohttp.ClientSession) -> None:
        fix_time = position.get("time")
        latlong = position.get("latlong")
        if not fix_time or not latlong:
            return
        if fix_time == self.last_fix_time:
            return
        lat, lng = latlong[0], latlong[1]
        device_id = self.device_id or self.dated_device_id()
        url = CALTOPO_URL.format(group=self.group.cfg.caltopo_group)
        async with http.get(url, params={"id": device_id, "lat": lat, "lng": lng}) as resp:
            body = await resp.text()
            if resp.status == 200:
                if self.last_fix_time is not None:
                    LOG.info("[%s] Fix interval: %ss", self.label,
                             int(fix_time - self.last_fix_time))
                self.last_fix_time = fix_time
                LOG.info("[%s] Posted %.6f,%.6f (fix %ss old)",
                         self.label, lat, lng, int(time.time() - fix_time))
            else:
                LOG.error("[%s] CalTopo rejected report: HTTP %s %s",
                          self.label, resp.status, body)

    async def keepalive_step(self) -> None:
        """One keepalive tick: re-arm a lost deployment, refresh a live one."""
        g = self.group
        if not self.intent_on:
            return
        if not self.live_active:
            offline = time.time() - (self.lost_since or time.time())
            if offline > g.rearm_max_seconds:
                LOG.warning("[%s] Giving up re-arming %s after %.0f min offline — "
                            "deployment considered over",
                            self.label, self.call_sign(), offline / 60)
                self._end_deployment()
                return
            try:
                await self.tracker.set_live_tracking_active(True)
                LOG.info("[%s] Re-arm sent (%.0fs offline)", self.label, offline)
            except TractiveError:
                LOG.info("[%s] Re-arm failed — collar still unreachable (%.0fs offline)",
                         self.label, offline)
            return
        if time.time() - self.last_keepalive < g.keepalive_seconds:
            return
        self.last_keepalive = time.time()
        elapsed = time.time() - (self.deployment_started or time.time())
        if g.max_live_seconds and elapsed > g.max_live_seconds:
            LOG.warning("[%s] Deployment exceeded %sh — turning LIVE off to save "
                        "collar battery", self.label, g.max_live_seconds // 3600)
            try:
                await self.tracker.set_live_tracking_active(False)
            except TractiveError:
                LOG.exception("[%s] Failed to turn LIVE off at cap", self.label)
            return
        try:
            await self.tracker.set_live_tracking_active(True)
            LOG.debug("[%s] LIVE keepalive sent", self.label)
        except TractiveError:
            LOG.exception("[%s] LIVE keepalive failed", self.label)


# ---- one group (one Tractive bridge account) ------------------------------

class GroupBridge:
    def __init__(self, cfg: GroupConfig, stop: asyncio.Event | None = None) -> None:
        self.cfg = cfg
        self.stop = stop or asyncio.Event()
        self.keepalive_seconds = int(os.environ.get("KEEPALIVE_SECONDS", "240"))
        self.max_live_seconds = int(os.environ.get("MAX_LIVE_SECONDS", "0"))  # 0 = no cap
        self.rearm_seconds = int(os.environ.get("REARM_SECONDS", "60"))
        self.rearm_max_seconds = int(os.environ.get("REARM_MAX_SECONDS", "7200"))
        self.dogs: dict[str, Dog] = {}   # tracker_id -> Dog
        self._ignored: set[str] = set()
        self._configured = [Dog(self, d) for d in cfg.dogs]

    def find_dog(self, name: str) -> Dog | None:
        key = name.lower()
        for dog in self.dogs.values():
            if key in (dog.name.lower(), dog.cfg.device_base.lower(), dog.tracker_id.lower()):
                return dog
        return None

    async def resolve(self, client: Tractive) -> None:
        """Attach trackers to dogs; enroll shared collars nobody configured."""
        trackers = await client.trackers()
        by_id = {t._id: t for t in trackers}
        self.dogs = {}
        for dog in self._configured:
            if dog.tracker_id:
                dog.tracker = by_id.get(dog.tracker_id) or client.tracker(dog.tracker_id)
                if dog.tracker_id not in by_id:
                    LOG.warning("[%s] Collar %s is not visible to this account — "
                                "was it unshared?", dog.label, dog.tracker_id)
            else:  # single-dog group: the account's only collar
                if len(trackers) != 1:
                    ids = ", ".join(by_id) or "none"
                    raise ConfigError(f"group {self.cfg.name!r}: expected exactly one "
                                      f"collar, found: {ids} — set tracker_id")
                dog.tracker = trackers[0]
                dog.tracker_id = trackers[0]._id
            self.dogs[dog.tracker_id] = dog
        if self.cfg.auto_enroll:
            unknown = [t for t in trackers if t._id not in self.dogs]
            names = await self._pet_names(client) if unknown else {}
            taken = {d.cfg.device_base.lower() for d in self.dogs.values()}
            for t in unknown:
                name = names.get(t._id) or t._id
                base = safe_name(name)
                if base.lower() in taken:
                    base = safe_name(f"{name}_{t._id[-4:]}")
                taken.add(base.lower())
                dog = Dog(self, DogConfig(name=name, device_base=base, tracker_id=t._id))
                dog.tracker = t
                self.dogs[t._id] = dog
                LOG.info("[%s] Auto-enrolled shared collar %s", dog.label, t._id)

    @staticmethod
    async def _pet_names(client: Tractive) -> dict[str, str]:
        """tracker_id -> pet name. Shared collars may come back without a name."""
        names: dict[str, str] = {}
        try:
            for obj in await client.trackable_objects():
                data = await obj.details()
                device_id = data.get("device_id")
                pet_name = (data.get("details") or {}).get("name")
                if device_id and pet_name:
                    names[device_id] = str(pet_name)
        except Exception:
            LOG.exception("Could not read pet names; using collar ids")
        return names

    # ---- worker ----

    async def run(self) -> None:
        LOG.info("[%s] Group up — OFF by default; LIVE is the switch "
                 "(call signs %s, keepalive %ss, max live %s)",
                 self.cfg.name,
                 "secured" if self.cfg.callsign_secret else "LEGACY/guessable",
                 self.keepalive_seconds,
                 f"{self.max_live_seconds // 3600}h" if self.max_live_seconds else "uncapped")
        while not self.stop.is_set():
            try:
                await self._session()
            except ConfigError as err:
                LOG.error("[%s] %s — retrying in 5 min", self.cfg.name, err)
                await self._sleep(300)
                continue
            except Exception:
                LOG.exception("[%s] Session dropped; reconnecting in 15s", self.cfg.name)
            await self._sleep(15)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _session(self) -> None:
        async with Tractive(self.cfg.tractive_email, self.cfg.tractive_password) as client:
            await self.resolve(client)
            LOG.info("[%s] Watching %d collar(s): %s", self.cfg.name, len(self.dogs),
                     ", ".join(d.name for d in self.dogs.values()) or "none")
            for dog in self.dogs.values():
                await dog.adopt_running_deployment()
            keepalive = asyncio.create_task(self._keepalive())
            try:
                async with aiohttp.ClientSession() as http:
                    async for event in client.events():
                        if self.stop.is_set():
                            break
                        await self.dispatch(event, http)
            finally:
                keepalive.cancel()

    async def dispatch(self, event: dict, http: aiohttp.ClientSession) -> None:
        if event.get("message") != "tracker_status":
            return
        tracker_id = event.get("tracker_id")
        if tracker_id is None:
            if len(self.dogs) != 1:
                return  # can't tell whose collar this is
            dog = next(iter(self.dogs.values()))
        else:
            dog = self.dogs.get(tracker_id)
            if dog is None:
                if tracker_id not in self._ignored:
                    self._ignored.add(tracker_id)
                    LOG.info("[%s] Events from collar %s, which isn't enrolled — "
                             "ignoring (newly shared? it is picked up on the next "
                             "reconnect if auto_enroll is on)", self.cfg.name, tracker_id)
                return
        try:
            await dog.handle_event(event, http)
        except Exception:  # one dog's bad event must not stall the others
            LOG.exception("[%s] Failed handling event", dog.label)

    async def _keepalive(self) -> None:
        try:
            while True:
                await asyncio.sleep(min(self.rearm_seconds, self.keepalive_seconds))
                for dog in list(self.dogs.values()):
                    try:
                        await dog.keepalive_step()
                    except Exception:
                        LOG.exception("[%s] Keepalive tick failed", dog.label)
        except asyncio.CancelledError:
            pass


# ---- one-shot commands ----------------------------------------------------

def parse_export_date(spec: str, tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    if spec == "today":
        return today
    if spec == "yesterday":
        return today - timedelta(days=1)
    for fmt in ("%y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(spec, fmt).replace(tzinfo=tz)
        except ValueError:
            continue
    sys.exit(f"Unrecognized date {spec!r} — use today, yesterday, yymmdd, or YYYY-MM-DD")


async def export_gpx(dog: Dog, spec: str, out: str | None) -> None:
    tz = dog.group.cfg.tz
    day = parse_export_date(spec, tz)
    t_from, t_to = day.timestamp(), (day + timedelta(days=1)).timestamp()
    data = await dog.tracker.positions(t_from, t_to, "json_segments")

    segments = data if isinstance(data, list) else data.get("positions", [])
    if segments and isinstance(segments[0], dict):
        segments = [segments]  # flat list of points -> one segment
    points = sum(len(s) for s in segments)
    if not points:
        sys.exit(f"No recorded positions for {dog.name} on {day.date()} — nothing to export.")

    def trkpt(p: dict) -> str:
        lat, lng = p["latlong"][0], p["latlong"][1]
        iso = datetime.fromtimestamp(p["time"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        ele = f"<ele>{p['alt']}</ele>" if p.get("alt") is not None else ""
        return f'      <trkpt lat="{lat}" lon="{lng}">{ele}<time>{iso}</time></trkpt>'

    name = f"{dog.cfg.device_base}-{day.strftime('%y%m%d')}"
    segs = "\n".join(
        "    <trkseg>\n" + "\n".join(trkpt(p) for p in seg if p.get("latlong")
                                     and p.get("time")) + "\n    </trkseg>"
        for seg in segments if seg
    )
    gpx = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<gpx version="1.1" creator="tractive-caltopo-bridge" '
           'xmlns="http://www.topografix.com/GPX/1/1">\n'
           f"  <trk>\n    <name>{name}</name>\n{segs}\n  </trk>\n</gpx>\n")
    path = os.path.abspath(out or f"{name}.gpx")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(gpx)
    first = datetime.fromtimestamp(segments[0][0]["time"], tz=ZoneInfo(tz))
    last = datetime.fromtimestamp(segments[-1][-1]["time"], tz=ZoneInfo(tz))
    print(f"Exported {points} points across {len(segments)} segment(s), "
          f"{first.strftime('%H:%M')}–{last.strftime('%H:%M %Z')} on {day.date()}")
    print(path)


async def print_status(dog: Dog) -> None:
    details = await dog.tracker.details()
    report = await dog.tracker.pos_report()
    latlong = report.get("latlong")
    age = int(time.time() - report["time"]) if report.get("time") else None
    # details() carries no live_tracking field, so LIVE is inferred from the
    # fix clock: 4s means LIVE, minutes means default mode.
    if age is None:
        live = "UNKNOWN (no fix on record)"
    elif age <= LIVE_FIX_AGE_SECONDS:
        live = f"ON (fix {age}s ago — 4s LIVE clock)"
    elif details.get("state_reason") == "OFF_PET_POWER_SAVING":
        live = f"OFF — collar powered off or off the dog ({age // 60} min silent)"
    else:
        live = f"OFF or out of cell — last fix {age // 60} min ago (default-mode interval)"
    try:
        pct = (await dog.tracker.hw_info()).get("battery_level")
    except Exception:
        pct = None
    battery = f"{pct}%" if pct is not None else details.get("battery_state")
    print(f"{dog.label}  tracker: {dog.tracker_id}  state: {details.get('state')} "
          f"({details.get('state_reason')})  battery: {battery}")
    print(f"  LIVE: {live}")
    print(f"  today's call sign: {dog.call_sign(dog.dated_device_id())}")
    if latlong:
        print(f"  last fix: {latlong[0]:.6f},{latlong[1]:.6f}  ({age}s ago)")


async def run_command(groups: list[GroupConfig], mode: str, group_sel: str | None,
                      dog_sel: str | None, export_spec: str | None, out: str | None) -> None:
    chosen = [g for g in groups if not group_sel or g.name.lower() == group_sel.lower()]
    if not chosen:
        sys.exit(f"No group named {group_sel!r} (have: {', '.join(g.name for g in groups)})")
    single_target = mode in ("on", "off", "once", "export")

    # Single-target commands: find exactly one dog first, act inside its session.
    if single_target:
        candidates = chosen
        if len(chosen) > 1:
            found: list[GroupConfig] = []
            for cfg in chosen:
                gb = GroupBridge(cfg)
                async with Tractive(cfg.tractive_email, cfg.tractive_password) as client:
                    await gb.resolve(client)
                hits = [gb.find_dog(dog_sel)] if dog_sel else list(gb.dogs.values())
                found.extend(cfg for d in hits if d)
            if len(found) != 1:
                sys.exit("No matching dog — see --list" if not found else
                         "More than one dog matches — pick one with --group/--dog")
            candidates = found
        cfg = candidates[0]
        gb = GroupBridge(cfg)
        async with Tractive(cfg.tractive_email, cfg.tractive_password) as client:
            await gb.resolve(client)
            dogs = [gb.find_dog(dog_sel)] if dog_sel else list(gb.dogs.values())
            dogs = [d for d in dogs if d]
            if len(dogs) != 1:
                names = ", ".join(d.name for d in gb.dogs.values()) or "none"
                sys.exit(f"Pick one dog in {cfg.name} with --dog (have: {names})")
            await _single(dogs[0], mode, export_spec, out)
        return

    for cfg in chosen:
        gb = GroupBridge(cfg)
        async with Tractive(cfg.tractive_email, cfg.tractive_password) as client:
            await gb.resolve(client)
            dogs = [gb.find_dog(dog_sel)] if dog_sel else list(gb.dogs.values())
            dogs = [d for d in dogs if d]
            if mode == "list":
                print(f"{cfg.name}  (CalTopo group {cfg.caltopo_group}, call signs "
                      f"{'secured' if cfg.callsign_secret else 'LEGACY/guessable'})")
                for d in dogs:
                    tag = "" if d in gb._configured else "  [auto-enrolled]"
                    print(f"  {d.name:<16} collar {d.tracker_id}  "
                          f"today: {d.call_sign(d.dated_device_id())}{tag}")
            else:  # status
                for d in dogs:
                    await print_status(d)


async def _single(dog: Dog, mode: str, export_spec: str | None, out: str | None) -> None:
    if mode == "on":
        state = await dog.tracker.set_live_tracking_active(True)
        LOG.info("LIVE tracking ON requested: %s", state)
        print(f"Deployment started for {dog.label} — today's call sign is "
              f"{dog.call_sign(dog.dated_device_id())}. Add it to the incident map "
              "as a live track (Fleet/Email/Other) EARLY; tracks do not backfill.")
    elif mode == "off":
        state = await dog.tracker.set_live_tracking_active(False)
        LOG.info("LIVE tracking OFF requested: %s", state)
        print(f"Deployment ended for {dog.label} — feed is silent. The track "
              "finalizes to a plain line on subscribed maps within ~24h.")
    elif mode == "once":
        report = await dog.tracker.pos_report()
        dog.intent_on = True
        async with aiohttp.ClientSession() as http:
            await dog.post({"time": report.get("time"), "latlong": report.get("latlong")}, http)
    elif mode == "export":
        await export_gpx(dog, export_spec or "today", out)


# ---- entry point ----------------------------------------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--list", action="store_true", help="list groups and dogs")
    action.add_argument("--on", action="store_true", help="start a deployment (enable LIVE)")
    action.add_argument("--off", action="store_true", help="end a deployment (disable LIVE)")
    action.add_argument("--status", action="store_true", help="show LIVE state + last fix")
    action.add_argument("--once", action="store_true", help="post latest fix once (test)")
    action.add_argument("--export", metavar="DATE",
                        help="export a day's positions as GPX "
                             "(today | yesterday | yymmdd | YYYY-MM-DD)")
    parser.add_argument("--group", metavar="NAME", help="which group (team)")
    parser.add_argument("--dog", metavar="NAME", help="which dog (name or collar id)")
    parser.add_argument("--out", metavar="PATH", help="output file for --export")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        groups = load_config()
    except ConfigError as err:
        sys.exit(str(err))

    mode = ("list" if args.list else "on" if args.on else "off" if args.off
            else "status" if args.status else "once" if args.once
            else "export" if args.export else None)
    if mode:
        try:
            await run_command(groups, mode, args.group, args.dog, args.export, args.out)
        except ConfigError as err:
            sys.exit(str(err))
        return

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    LOG.info("Bridge worker: %d group(s) — %s", len(groups), ", ".join(g.name for g in groups))
    await asyncio.gather(*(GroupBridge(g, stop).run() for g in groups))


if __name__ == "__main__":
    asyncio.run(main())
