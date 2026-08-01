#!/usr/bin/env python3
"""Tractive -> CalTopo position bridge.

Polls a Tractive GPS collar and reports each NEW fix to a CalTopo live track
(Fleet/Email/Other position report endpoint). Designed to run as a long-lived
background worker (Sevalla) or locally for testing.

Modes:
  idle  (default) - slow poll, collar stays in its battery-saving cadence.
  live            - enables Tractive LIVE tracking (fix every few seconds,
                    heavy battery drain) and polls fast. Use during a search.

Only posts to CalTopo when the fix timestamp changes, so sparse collar
updates never produce duplicate points on the map.

All configuration comes from environment variables (see .env.example).
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
        self.poll_idle = int(os.environ.get("POLL_IDLE_SECONDS", "60"))
        self.poll_live = int(os.environ.get("POLL_LIVE_SECONDS", "15"))
        self.live = os.environ.get("MODE", "idle").lower() == "live"
        self.last_fix_time: float | None = None
        self.stop = asyncio.Event()

    async def run(self) -> None:
        async with Tractive(self.email, self.password) as client:
            tracker = await self._resolve_tracker(client)
            if self.live:
                LOG.info("Enabling LIVE tracking (battery drain: hours, not days)")
                await tracker.set_live_tracking_active(True)
            try:
                await self._poll_loop(tracker)
            finally:
                if self.live:
                    LOG.info("Disabling LIVE tracking")
                    try:
                        await tracker.set_live_tracking_active(False)
                    except Exception:
                        LOG.exception("Could not disable LIVE mode — check the Tractive app")

    async def _resolve_tracker(self, client: Tractive):
        if self.tracker_id:
            return client.tracker(self.tracker_id)
        trackers = await client.trackers()
        if not trackers:
            sys.exit("No trackers on this Tractive account")
        if len(trackers) > 1:
            ids = ", ".join(t._id for t in trackers)
            sys.exit(f"Multiple trackers found ({ids}) — set TRACTIVE_TRACKER_ID")
        tracker = trackers[0]
        LOG.info("Using tracker %s", tracker._id)
        return tracker

    async def _poll_loop(self, tracker) -> None:
        interval = self.poll_live if self.live else self.poll_idle
        LOG.info(
            "Polling every %ss (%s mode), reporting %s-%s to CalTopo",
            interval, "live" if self.live else "idle", self.group, self.device_id,
        )
        async with aiohttp.ClientSession() as http:
            while not self.stop.is_set():
                try:
                    await self._poll_once(tracker, http)
                except Exception:
                    LOG.exception("Poll failed; retrying next cycle")
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass

    async def _poll_once(self, tracker, http: aiohttp.ClientSession) -> None:
        report = await tracker.pos_report()
        fix_time = report.get("time")
        latlong = report.get("latlong")
        if not fix_time or not latlong:
            LOG.warning("No position in report: %s", report)
            return
        if fix_time == self.last_fix_time:
            return
        if self.last_fix_time is not None:
            LOG.info("Fix interval observed: %ss", int(fix_time - self.last_fix_time))
        lat, lng = latlong[0], latlong[1]
        url = CALTOPO_URL.format(group=self.group)
        async with http.get(
            url, params={"id": self.device_id, "lat": lat, "lng": lng}
        ) as resp:
            body = await resp.text()
            if resp.status == 200:
                self.last_fix_time = fix_time
                age = int(time.time() - fix_time)
                LOG.info("Posted %.6f,%.6f (fix %ss old)", lat, lng, age)
            else:
                LOG.error("CalTopo rejected report: HTTP %s %s", resp.status, body)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="search mode: LIVE tracking + fast poll")
    parser.add_argument("--once", action="store_true", help="poll and report a single fix, then exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bridge = Bridge()
    if args.live:
        bridge.live = True

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, bridge.stop.set)

    if args.once:
        async with Tractive(bridge.email, bridge.password) as client:
            tracker = await bridge._resolve_tracker(client)
            async with aiohttp.ClientSession() as http:
                await bridge._poll_once(tracker, http)
        return

    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
