#!/usr/bin/env python3
"""Tractive -> CalTopo position bridge.

OFF BY DEFAULT. The switch is Tractive's own LIVE mode:

- Turn LIVE tracking on (in the Tractive app, or `bridge.py --on`) and the
  bridge relays every position fix to a CalTopo live track.
- Turn LIVE off (app, or `bridge.py --off`) and the bridge goes silent.
  No positions are reported outside a deployment.

Each deployment posts to a DATED device id ({DEVICE_ID}-yymmdd, minted when
the deployment starts), so every deployment is its own CalTopo track. A map
that subscribed to one deployment's call sign never sees the next one; the
old live track finalizes into a plain line ~24h after its last fix. Date is
taken in BRIDGE_TZ (default America/Los_Angeles).

The worker holds Tractive's real-time event channel open, so the switch
takes effect within seconds, and positions arrive pushed (no polling).
While the bridge believes a deployment is on it re-issues the LIVE command
before Tractive's ~300s timeout expires, so LIVE stays on until someone
turns it off (app or --off) — a deployment never times out mid-search. Set
MAX_LIVE_SECONDS to add a hard battery-protection cap (0 = none, the
default; the collar battery itself is the natural limit).

Commands:
  bridge.py                run the relay worker (deploy this)
  bridge.py --on           start a deployment: enable LIVE tracking, then exit
  bridge.py --off          end a deployment: disable LIVE tracking, then exit
  bridge.py --status       show LIVE state and last known position, then exit
  bridge.py --once         post the latest fix to CalTopo once (test), then exit
  bridge.py --export DATE  export a day's recorded positions as a GPX file.
                           DATE = today | yesterday | yymmdd | YYYY-MM-DD.
                           Pulls from Tractive's stored history, so it works
                           even for days with no cell coverage in the field —
                           the collar uploads its offline fixes once it's back
                           in coverage. Writes {DEVICE_ID}-{yymmdd}.gpx (or
                           --out PATH). The file is yours to share or import;
                           nothing touches any CalTopo map.

Configuration via environment variables (see .env.example).
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
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

# One-off mid-day track splits, keyed by Pacific date (yymmdd). The dated call
# sign alone can't start a second track on the same day, so an entry here adds
# a suffix for that date only and the scheme reverts to the plain
# <base>-yymmdd call sign at midnight without another deploy.
# CALTOPO_TRACK_SUFFIX overrides this if set.
TRACK_SUFFIXES = {
    "260809": "b",  # search track split off from the drive-out track
}


# A fix this fresh can only come from the ~4s LIVE clock; default mode is
# ~2-3 min apart. Used both to adopt an in-progress deployment on boot and to
# tell a deliberate LIVE-off from one caused by losing cell.
LIVE_FIX_AGE_SECONDS = 20


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


class Bridge:
    def __init__(self) -> None:
        self.email = require_env("TRACTIVE_EMAIL")
        self.password = require_env("TRACTIVE_PASSWORD")
        self.group = require_env("CALTOPO_GROUP")
        self.device_base = require_env("CALTOPO_DEVICE_ID")
        self.tz = os.environ.get("BRIDGE_TZ", "America/Los_Angeles")
        self.device_id: str | None = None  # minted per deployment
        self.tracker_id = os.environ.get("TRACTIVE_TRACKER_ID", "").strip()
        self.keepalive_seconds = int(os.environ.get("KEEPALIVE_SECONDS", "240"))
        self.max_live_seconds = int(os.environ.get("MAX_LIVE_SECONDS", "0"))  # 0 = no cap
        # How often to re-arm LIVE after cell loss, and how long to keep trying
        # before assuming the deployment is really over.
        self.rearm_seconds = int(os.environ.get("REARM_SECONDS", "60"))
        self.rearm_max_seconds = int(os.environ.get("REARM_MAX_SECONDS", "7200"))
        # intent_on = a deployment is meant to be running (survives cell loss).
        # live_active = LIVE is actually up right now. Losing cell clears the
        # second, never the first — only an operator stop clears intent.
        self.intent_on = False
        self.live_active = False
        self.lost_since: float | None = None
        self.last_keepalive: float = 0.0
        self.deployment_started: float | None = None
        self.last_fix_time: float | None = None
        self.stop = asyncio.Event()

    async def resolve_tracker(self, client: Tractive):
        if self.tracker_id:
            return client.tracker(self.tracker_id)
        trackers = await client.trackers()
        if not trackers:
            sys.exit("No trackers on this Tractive account")
        if len(trackers) > 1:
            ids = ", ".join(t._id for t in trackers)
            sys.exit(f"Multiple trackers found ({ids}) — set TRACTIVE_TRACKER_ID")
        return trackers[0]

    def dated_device_id(self) -> str:
        day = datetime.now(ZoneInfo(self.tz)).strftime("%y%m%d")
        suffix = (os.environ.get("CALTOPO_TRACK_SUFFIX", "").strip()
                  or TRACK_SUFFIXES.get(day, ""))
        return f"{self.device_base}-{day}-{suffix}" if suffix else f"{self.device_base}-{day}"

    def call_sign(self, device_id: str) -> str:
        return f"{self.group}-{device_id}"

    # ---- worker -----------------------------------------------------------

    async def run(self) -> None:
        LOG.info(
            "Bridge up — OFF by default; LIVE tracking is the switch "
            "(feed %s-%s-yymmdd, keepalive %ss, max live %s)",
            self.group, self.device_base, self.keepalive_seconds,
            f"{self.max_live_seconds // 3600}h" if self.max_live_seconds else "uncapped",
        )
        while not self.stop.is_set():
            try:
                await self._session()
            except Exception:
                LOG.exception("Session dropped; reconnecting in 15s")
            if not self.stop.is_set():
                await asyncio.sleep(15)

    async def _session(self) -> None:
        async with Tractive(self.email, self.password) as client:
            tracker = await self.resolve_tracker(client)
            await self._adopt_running_deployment(tracker)
            keepalive = asyncio.create_task(self._keepalive(tracker))
            try:
                async with aiohttp.ClientSession() as http:
                    async for event in client.events():
                        if self.stop.is_set():
                            break
                        await self._handle_event(event, http)
            finally:
                keepalive.cancel()

    async def _adopt_running_deployment(self, tracker) -> None:
        """Pick up a deployment that is already running when the worker boots.

        LIVE state only ever arrives as an event, so a restart mid-deployment
        (a redeploy, a crash) would otherwise leave the bridge silent until
        someone toggled LIVE by hand. A fix on the 4s clock means it's live.
        """
        try:
            report = await tracker.pos_report()
        except Exception:
            LOG.exception("Could not check for a running deployment on boot")
            return
        fix_time = report.get("time")
        if not fix_time:
            return
        age = time.time() - fix_time
        if age <= LIVE_FIX_AGE_SECONDS:
            self.intent_on = True
            self.live_active = True
            self.deployment_started = time.time()
            self.device_id = self.dated_device_id()
            LOG.info("Adopted a deployment already in progress (fix %.0fs old) "
                     "— call sign %s", age, self.call_sign(self.device_id))

    async def _handle_event(self, event: dict, http: aiohttp.ClientSession) -> None:
        if event.get("message") != "tracker_status":
            return
        if self.tracker_id and event.get("tracker_id") not in (None, self.tracker_id):
            return

        live = event.get("live_tracking")
        if live is not None:
            on = bool(live.get("active") or live.get("pending"))
            if on:
                if not self.intent_on:
                    self.intent_on = True
                    self.deployment_started = time.time()
                    self.device_id = self.dated_device_id()
                    LOG.info("DEPLOYMENT ON — LIVE tracking active, call sign %s",
                             self.call_sign(self.device_id))
                elif not self.live_active:
                    offline = time.time() - (self.lost_since or time.time())
                    LOG.info("LIVE RE-ARMED after %.0fs offline — same track %s",
                             offline, self.call_sign(self.device_id or "?"))
                self.live_active = True
                self.lost_since = None
            elif self.intent_on and self.live_active:
                # Losing cell and being switched off both arrive as LIVE-off.
                # Treat it as a stop only when the collar is demonstrably still
                # reachable; otherwise the deployment stays armed and the
                # keepalive keeps re-arming until coverage returns.
                stale = (self.last_fix_time is None
                         or time.time() - self.last_fix_time > LIVE_FIX_AGE_SECONDS)
                if live.get("reconnecting") or stale:
                    self.live_active = False
                    self.lost_since = time.time()
                    LOG.warning(
                        "LIVE LOST (collar unreachable — no cell?) — track %s stays "
                        "armed, re-arming every %ss for up to %.0f min",
                        self.call_sign(self.device_id or "?"), self.rearm_seconds,
                        self.rearm_max_seconds / 60)
                else:
                    LOG.info("DEPLOYMENT OFF — operator stop, feed silent "
                             "(track %s is done)", self.call_sign(self.device_id or "?"))
                    self.intent_on = False
                    self.live_active = False
                    self.lost_since = None
                    self.deployment_started = None
                    self.device_id = None

        position = event.get("position")
        # Post whatever arrives while a deployment is armed: during a re-arm the
        # collar reports at the coarse default interval, and a sparse track beats
        # a dead one.
        if position and self.intent_on:
            await self._post(position, http)

    async def _post(self, position: dict, http: aiohttp.ClientSession) -> None:
        fix_time = position.get("time")
        latlong = position.get("latlong")
        if not fix_time or not latlong:
            return
        if fix_time == self.last_fix_time:
            return
        lat, lng = latlong[0], latlong[1]
        device_id = self.device_id or self.dated_device_id()
        url = CALTOPO_URL.format(group=self.group)
        async with http.get(
            url, params={"id": device_id, "lat": lat, "lng": lng}
        ) as resp:
            body = await resp.text()
            if resp.status == 200:
                if self.last_fix_time is not None:
                    LOG.info("Fix interval: %ss", int(fix_time - self.last_fix_time))
                self.last_fix_time = fix_time
                LOG.info("Posted %.6f,%.6f (fix %ss old)",
                         lat, lng, int(time.time() - fix_time))
            else:
                LOG.error("CalTopo rejected report: HTTP %s %s", resp.status, body)

    async def _keepalive(self, tracker) -> None:
        """Refresh LIVE while a deployment is on; hard-stop at max_live_seconds."""
        try:
            while True:
                # Poll at the re-arm cadence; a live deployment only needs a
                # refresh every keepalive_seconds, but a lost one wants picking
                # up the moment coverage returns.
                await asyncio.sleep(min(self.rearm_seconds, self.keepalive_seconds))
                if not self.intent_on:
                    continue
                if not self.live_active:
                    offline = time.time() - (self.lost_since or time.time())
                    if offline > self.rearm_max_seconds:
                        LOG.warning(
                            "Giving up re-arming %s after %.0f min offline — "
                            "deployment considered over",
                            self.call_sign(self.device_id or "?"), offline / 60)
                        self.intent_on = False
                        self.lost_since = None
                        self.device_id = None
                        continue
                    try:
                        await tracker.set_live_tracking_active(True)
                        LOG.info("Re-arm sent (%.0fs offline)", offline)
                    except TractiveError:
                        LOG.info("Re-arm failed — collar still unreachable "
                                 "(%.0fs offline)", offline)
                    continue
                if time.time() - self.last_keepalive < self.keepalive_seconds:
                    continue
                self.last_keepalive = time.time()
                elapsed = time.time() - (self.deployment_started or time.time())
                if self.max_live_seconds and elapsed > self.max_live_seconds:
                    LOG.warning(
                        "Deployment exceeded %sh — turning LIVE off to save collar battery",
                        self.max_live_seconds // 3600,
                    )
                    try:
                        await tracker.set_live_tracking_active(False)
                    except TractiveError:
                        LOG.exception("Failed to turn LIVE off at cap")
                    continue
                try:
                    await tracker.set_live_tracking_active(True)
                    LOG.debug("LIVE keepalive sent")
                except TractiveError:
                    LOG.exception("LIVE keepalive failed")
        except asyncio.CancelledError:
            pass

    # ---- one-shot commands ------------------------------------------------

    def parse_export_date(self, spec: str) -> datetime:
        tz = ZoneInfo(self.tz)
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

    async def export(self, spec: str, out: str | None) -> None:
        day = self.parse_export_date(spec)
        t_from, t_to = day.timestamp(), (day + timedelta(days=1)).timestamp()
        async with Tractive(self.email, self.password) as client:
            tracker = await self.resolve_tracker(client)
            data = await tracker.positions(t_from, t_to, "json_segments")

        segments = data if isinstance(data, list) else data.get("positions", [])
        if segments and isinstance(segments[0], dict):
            segments = [segments]  # flat list of points -> one segment
        points = sum(len(s) for s in segments)
        if not points:
            sys.exit(f"No recorded positions for {day.date()} — nothing to export.")

        def trkpt(p: dict) -> str:
            lat, lng = p["latlong"][0], p["latlong"][1]
            iso = datetime.fromtimestamp(p["time"], tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            ele = f"<ele>{p['alt']}</ele>" if p.get("alt") is not None else ""
            return f'      <trkpt lat="{lat}" lon="{lng}">{ele}<time>{iso}</time></trkpt>'

        name = f"{self.device_base}-{day.strftime('%y%m%d')}"
        segs = "\n".join(
            "    <trkseg>\n" + "\n".join(trkpt(p) for p in seg if p.get("latlong")
                                         and p.get("time")) + "\n    </trkseg>"
            for seg in segments if seg
        )
        gpx = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<gpx version="1.1" creator="tractive-caltopo-bridge" '
            'xmlns="http://www.topografix.com/GPX/1/1">\n'
            f"  <trk>\n    <name>{name}</name>\n{segs}\n  </trk>\n</gpx>\n"
        )
        path = os.path.abspath(out or f"{name}.gpx")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(gpx)
        first = datetime.fromtimestamp(segments[0][0]["time"], tz=ZoneInfo(self.tz))
        last = datetime.fromtimestamp(segments[-1][-1]["time"], tz=ZoneInfo(self.tz))
        print(f"Exported {points} points across {len(segments)} segment(s), "
              f"{first.strftime('%H:%M')}–{last.strftime('%H:%M %Z')} on {day.date()}")
        print(path)

    async def command(self, mode: str) -> None:
        async with Tractive(self.email, self.password) as client:
            tracker = await self.resolve_tracker(client)
            if mode == "on":
                state = await tracker.set_live_tracking_active(True)
                LOG.info("LIVE tracking ON requested: %s", state)
                print(f"Deployment started — today's call sign is "
                      f"{self.call_sign(self.dated_device_id())}. Add it to the "
                      "incident map as a live track (Fleet/Email/Other) EARLY; "
                      "tracks do not backfill.")
            elif mode == "off":
                state = await tracker.set_live_tracking_active(False)
                LOG.info("LIVE tracking OFF requested: %s", state)
                print("Deployment ended — feed is silent. The track finalizes "
                      "to a plain line on subscribed maps within ~24h.")
            elif mode == "status":
                details = await tracker.details()
                report = await tracker.pos_report()
                latlong = report.get("latlong")
                age = int(time.time() - report["time"]) if report.get("time") else None
                # details() carries no live_tracking field, so LIVE is inferred
                # from the fix clock: 4s means LIVE, minutes means default mode.
                if age is None:
                    live = "UNKNOWN (no fix on record)"
                elif age <= LIVE_FIX_AGE_SECONDS:
                    live = f"ON (fix {age}s ago — 4s LIVE clock)"
                elif details.get("state_reason") == "OFF_PET_POWER_SAVING":
                    live = f"OFF — collar powered off or off the dog ({age // 60} min silent)"
                else:
                    live = (f"OFF or out of cell — last fix {age // 60} min ago "
                            "(default-mode interval, not LIVE)")
                try:
                    pct = (await tracker.hw_info()).get("battery_level")
                except Exception:
                    pct = None
                battery = f"{pct}%" if pct is not None else details.get("battery_state")
                print(f"tracker: {tracker._id}  state: {details.get('state')} "
                      f"({details.get('state_reason')})  battery: {battery}")
                print(f"LIVE: {live}")
                print(f"today's call sign: {self.call_sign(self.dated_device_id())}")
                if latlong:
                    print(f"last fix: {latlong[0]:.6f},{latlong[1]:.6f}  ({age}s ago)")
            elif mode == "once":
                async with aiohttp.ClientSession() as http:
                    self.intent_on = True
                    await self._post(report_from(await tracker.pos_report()), http)


def report_from(pos_report: dict) -> dict:
    return {"time": pos_report.get("time"), "latlong": pos_report.get("latlong")}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--on", action="store_true", help="start a deployment (enable LIVE)")
    group.add_argument("--off", action="store_true", help="end a deployment (disable LIVE)")
    group.add_argument("--status", action="store_true", help="show LIVE state + last fix")
    group.add_argument("--once", action="store_true", help="post latest fix once (test)")
    group.add_argument("--export", metavar="DATE",
                       help="export a day's positions as GPX "
                            "(today | yesterday | yymmdd | YYYY-MM-DD)")
    parser.add_argument("--out", metavar="PATH",
                        help="output file for --export (default {DEVICE_ID}-{yymmdd}.gpx)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bridge = Bridge()

    if args.export:
        await bridge.export(args.export, args.out)
        return
    if args.on or args.off or args.status or args.once:
        mode = "on" if args.on else "off" if args.off else "status" if args.status else "once"
        await bridge.command(mode)
        return

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, bridge.stop.set)
    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
