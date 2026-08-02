#!/usr/bin/env python3
"""Tractive -> CalTopo position bridge.

OFF BY DEFAULT. The switch is Tractive's own LIVE mode:

- Turn LIVE tracking on (in the Tractive app, or `bridge.py --on`) and the
  bridge relays every position fix to a CalTopo live track.
- Turn LIVE off (app, or `bridge.py --off`) and the bridge goes silent.
  No positions are reported outside a deployment.

The worker holds Tractive's real-time event channel open, so the switch
takes effect within seconds, and positions arrive pushed (no polling).
While the bridge believes a deployment is on it re-issues the LIVE command
before Tractive's ~300s timeout expires, so LIVE started remotely (with no
phone app keeping it alive) stays on — capped at MAX_LIVE_SECONDS (default
8h) to protect the collar battery if someone forgets to turn it off.

Commands:
  bridge.py           run the relay worker (deploy this)
  bridge.py --on      start a deployment: enable LIVE tracking, then exit
  bridge.py --off     end a deployment: disable LIVE tracking, then exit
  bridge.py --status  show LIVE state and last known position, then exit
  bridge.py --once    post the latest fix to CalTopo once (test), then exit

Configuration via environment variables (see .env.example).
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

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
        self.device_id = require_env("CALTOPO_DEVICE_ID")
        self.tracker_id = os.environ.get("TRACTIVE_TRACKER_ID", "").strip()
        self.keepalive_seconds = int(os.environ.get("KEEPALIVE_SECONDS", "240"))
        self.max_live_seconds = int(os.environ.get("MAX_LIVE_SECONDS", "28800"))
        self.deployment_on = False
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

    # ---- worker -----------------------------------------------------------

    async def run(self) -> None:
        LOG.info(
            "Bridge up — OFF by default; LIVE tracking is the switch "
            "(feed %s-%s, keepalive %ss, max live %sh)",
            self.group, self.device_id, self.keepalive_seconds,
            self.max_live_seconds // 3600,
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
            keepalive = asyncio.create_task(self._keepalive(tracker))
            try:
                async with aiohttp.ClientSession() as http:
                    async for event in client.events():
                        if self.stop.is_set():
                            break
                        await self._handle_event(event, http)
            finally:
                keepalive.cancel()

    async def _handle_event(self, event: dict, http: aiohttp.ClientSession) -> None:
        if event.get("message") != "tracker_status":
            return
        if self.tracker_id and event.get("tracker_id") not in (None, self.tracker_id):
            return

        live = event.get("live_tracking")
        if live is not None:
            on = bool(live.get("active") or live.get("pending"))
            if on and not self.deployment_on:
                self.deployment_on = True
                self.deployment_started = time.time()
                LOG.info("DEPLOYMENT ON — LIVE tracking active, relaying to %s-%s",
                         self.group, self.device_id)
            elif not on and self.deployment_on:
                self.deployment_on = False
                self.deployment_started = None
                LOG.info("DEPLOYMENT OFF — LIVE tracking ended, feed silent")

        position = event.get("position")
        if position and self.deployment_on:
            await self._post(position, http)

    async def _post(self, position: dict, http: aiohttp.ClientSession) -> None:
        fix_time = position.get("time")
        latlong = position.get("latlong")
        if not fix_time or not latlong:
            return
        if fix_time == self.last_fix_time:
            return
        lat, lng = latlong[0], latlong[1]
        url = CALTOPO_URL.format(group=self.group)
        async with http.get(
            url, params={"id": self.device_id, "lat": lat, "lng": lng}
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
                await asyncio.sleep(self.keepalive_seconds)
                if not self.deployment_on:
                    continue
                elapsed = time.time() - (self.deployment_started or time.time())
                if elapsed > self.max_live_seconds:
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

    async def command(self, mode: str) -> None:
        async with Tractive(self.email, self.password) as client:
            tracker = await self.resolve_tracker(client)
            if mode == "on":
                state = await tracker.set_live_tracking_active(True)
                LOG.info("LIVE tracking ON requested: %s", state)
                print("Deployment started — the worker will relay positions "
                      "while LIVE stays on.")
            elif mode == "off":
                state = await tracker.set_live_tracking_active(False)
                LOG.info("LIVE tracking OFF requested: %s", state)
                print("Deployment ended — feed is silent.")
            elif mode == "status":
                details = await tracker.details()
                report = await tracker.pos_report()
                latlong = report.get("latlong")
                age = int(time.time() - report["time"]) if report.get("time") else None
                print(f"tracker: {tracker._id}  state: {details.get('state')} "
                      f"({details.get('state_reason')})  battery: {details.get('battery_state')}")
                if latlong:
                    print(f"last fix: {latlong[0]:.6f},{latlong[1]:.6f}  ({age}s ago)")
            elif mode == "once":
                async with aiohttp.ClientSession() as http:
                    self.deployment_on = True
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
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bridge = Bridge()

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
