"""Offline tests: fake Tractive collars, fake CalTopo. No network, no accounts."""

import asyncio
import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bridge  # noqa: E402
from bridge import (ConfigError, DogConfig, GroupBridge, GroupConfig,  # noqa: E402
                    callsign_tag, load_config, parse_config)


# ---- fakes ----------------------------------------------------------------

class FakeResp:
    def __init__(self, status=200):
        self.status = status

    async def text(self):
        return "ok"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class FakeHttp:
    def __init__(self):
        self.gets = []   # (url, params)
        self.posts = []  # (url, kwargs)

    def get(self, url, params=None):
        self.gets.append((url, params))
        return FakeResp()

    def post(self, url, timeout=None, **kwargs):
        self.posts.append((url, kwargs))
        return FakeResp()


class FakeTracker:
    def __init__(self, tid, pos_time=None):
        self._id = tid
        self.pos_time = pos_time
        self.live_calls = []

    async def pos_report(self):
        return {"time": self.pos_time, "latlong": [34.24, -116.91]}

    async def set_live_tracking_active(self, on):
        self.live_calls.append(on)
        return {"ok": True}


class FakeObj:
    def __init__(self, tid, name):
        self.tid, self.name = tid, name

    async def details(self):
        return {"device_id": self.tid, "details": {"name": self.name}}


class FakeClient:
    def __init__(self, trackers, names=None):
        self._trackers = trackers
        self._names = names or {}

    async def trackers(self):
        return self._trackers

    def tracker(self, tid):
        return FakeTracker(tid)

    async def trackable_objects(self):
        return [FakeObj(t, n) for t, n in self._names.items()]


def group(dogs=None, secret="s3cret", auto=True, name="SBSTAR", key="KEY1"):
    return GroupConfig(name=name, tractive_email="b@x", tractive_password="p",
                       caltopo_group=key, callsign_secret=secret,
                       notify=["https://ntfy.sh/topic"], auto_enroll=auto,
                       dogs=dogs or [])


def ev(tid, live=None, pos_time=None):
    e = {"message": "tracker_status", "tracker_id": tid}
    if live is not None:
        e["live_tracking"] = live
    if pos_time is not None:
        e["position"] = {"time": pos_time, "latlong": [34.2, -116.9]}
    return e


def run(coro):
    return asyncio.run(coro)


# ---- call signs -----------------------------------------------------------

def test_tag_stable_and_distinct():
    a = callsign_tag("s", "TRK1", "261114")
    assert a == callsign_tag("s", "TRK1", "261114")      # restart-stable
    assert a != callsign_tag("s", "TRK2", "261114")      # per dog
    assert a != callsign_tag("s", "TRK1", "261115")      # per day
    assert a != callsign_tag("other", "TRK1", "261114")  # per team secret
    assert len(a) == 5 and a.isalnum() and "-" not in a


def test_secured_call_sign_shape():
    gb = GroupBridge(group([DogConfig("Storm", "Storm", "TRK1")]))
    dog = gb._configured[0]
    cs = dog.call_sign(dog.dated_device_id())
    key, base, day, tag = cs.split("-")
    assert (key, base) == ("KEY1", "Storm") and len(day) == 6 and len(tag) == 5


def test_legacy_call_sign_unchanged(monkeypatch):
    for k, v in {"TRACTIVE_EMAIL": "e", "TRACTIVE_PASSWORD": "p",
                 "CALTOPO_GROUP": "TEAM1", "CALTOPO_DEVICE_ID": "Dog"}.items():
        monkeypatch.setenv(k, v)
    for k in ("BRIDGE_CONFIG", "CALLSIGN_SECRET", "CALTOPO_TRACK_SUFFIX"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(bridge, "TRACK_SUFFIXES", {})
    [cfg] = load_config()
    assert cfg.legacy and not cfg.callsign_secret
    dog = GroupBridge(cfg)._configured[0]
    assert dog.label == "Dog"
    assert len(dog.call_sign(dog.dated_device_id()).split("-")) == 3  # TEAM1-Dog-yymmdd


# ---- config ---------------------------------------------------------------

def test_config_rejects_hyphen_and_missing_ids(monkeypatch):
    base = {"name": "A", "tractive_email": "e", "tractive_password": "p"}
    with pytest.raises(ConfigError):
        parse_config({"groups": [{**base, "caltopo_group": "AB-C"}]})
    with pytest.raises(ConfigError):
        parse_config({"groups": [{**base, "caltopo_group": "K",
                                  "dogs": [{"name": "A"}, {"name": "B"}]}]})


def test_config_secrets_from_env(monkeypatch):
    monkeypatch.setenv("SB_PW", "pw")
    monkeypatch.setenv("SB_SECRET", "sec")
    [g] = parse_config({"groups": [{
        "name": "SBSTAR", "tractive_email": "b@x", "tractive_password_env": "SB_PW",
        "caltopo_group": "K", "callsign_secret_env": "SB_SECRET",
        "dogs": [{"name": "Storm Cloud", "tracker_id": "T1"}]}]})
    assert g.tractive_password == "pw" and g.callsign_secret == "sec"
    assert g.dogs[0].device_base == "Storm_Cloud"


def test_config_from_json_env(monkeypatch):
    monkeypatch.setenv("BRIDGE_CONFIG", json.dumps({"groups": [
        {"name": "A", "tractive_email": "e", "tractive_password": "p", "caltopo_group": "K1"},
        {"name": "B", "tractive_email": "e", "tractive_password": "p", "caltopo_group": "K2"}]}))
    assert [g.name for g in load_config()] == ["A", "B"]


# ---- enrollment -----------------------------------------------------------

def test_resolve_configured_and_auto_enrolled():
    gb = GroupBridge(group([DogConfig("Storm", "Storm", "T1")]))
    client = FakeClient([FakeTracker("T1"), FakeTracker("T2"), FakeTracker("T3")],
                        names={"T2": "Kona"})
    run(gb.resolve(client))
    assert set(gb.dogs) == {"T1", "T2", "T3"}
    assert gb.dogs["T2"].name == "Kona"
    assert gb.dogs["T3"].name == "T3"  # shared collar with no visible pet name


def test_auto_enroll_off_ignores_unlisted():
    gb = GroupBridge(group([DogConfig("Storm", "Storm", "T1")], auto=False))
    run(gb.resolve(FakeClient([FakeTracker("T1"), FakeTracker("T2")])))
    assert set(gb.dogs) == {"T1"}


# ---- deployments ----------------------------------------------------------

def make_two_dogs():
    gb = GroupBridge(group())
    run(gb.resolve(FakeClient([FakeTracker("T1"), FakeTracker("T2")],
                              names={"T1": "Storm", "T2": "Kona"})))
    return gb


def test_two_dogs_route_to_own_tracks_and_notify():
    gb, http = make_two_dogs(), FakeHttp()
    now = time.time()

    async def go():
        await gb.dispatch(ev("T1", live={"active": True}), http)
        await gb.dispatch(ev("T1", pos_time=now), http)
        await gb.dispatch(ev("T2", pos_time=now), http)  # Kona not deployed
    run(go())

    storm, kona = gb.dogs["T1"], gb.dogs["T2"]
    assert storm.intent_on and not kona.intent_on
    assert len(http.gets) == 1 and http.gets[0][1]["id"] == storm.device_id
    assert http.gets[0][0].endswith("/KEY1")
    assert len(http.posts) == 1 and storm.call_sign().encode() in http.posts[0][1]["data"]


def test_cell_loss_keeps_deployment_and_operator_stop_ends_it():
    gb, http = make_two_dogs(), FakeHttp()
    dog = gb.dogs["T1"]

    async def go():
        await gb.dispatch(ev("T1", live={"active": True}), http)
        first_id = dog.device_id
        # LIVE-off with a stale fix = lost cell: stays armed, same call sign
        dog.last_fix_time = time.time() - 300
        await gb.dispatch(ev("T1", live={"active": False}), http)
        assert dog.intent_on and not dog.live_active
        await gb.dispatch(ev("T1", live={"active": True}), http)  # coverage back
        assert dog.device_id == first_id and dog.live_active
        assert len(http.posts) == 1  # re-arm doesn't re-notify
        # LIVE-off with a fresh fix = operator stop
        dog.last_fix_time = time.time()
        await gb.dispatch(ev("T1", live={"active": False}), http)
        assert not dog.intent_on and dog.device_id is None
    run(go())


def test_keepalive_rearms_lost_dog_only():
    gb = make_two_dogs()
    storm, kona = gb.dogs["T1"], gb.dogs["T2"]
    storm.intent_on, storm.live_active, storm.lost_since = True, False, time.time()
    run(storm.keepalive_step())
    run(kona.keepalive_step())
    assert storm.tracker.live_calls == [True] and kona.tracker.live_calls == []


def test_adopt_running_deployment_on_boot():
    gb = GroupBridge(group())
    run(gb.resolve(FakeClient([FakeTracker("T1", pos_time=time.time() - 3),
                               FakeTracker("T2", pos_time=time.time() - 600)])))
    for d in gb.dogs.values():
        run(d.adopt_running_deployment())
    assert gb.dogs["T1"].intent_on and not gb.dogs["T2"].intent_on


def test_unknown_collar_event_is_ignored():
    gb, http = make_two_dogs(), FakeHttp()
    run(gb.dispatch(ev("T9", live={"active": True}), http))
    assert http.gets == [] and http.posts == []
