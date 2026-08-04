# tractive-caltopo-bridge

Put a SAR dog's Tractive GPS collar on a live CalTopo map — **only while
you're actually deployed.**

Search-and-rescue teams run their incident maps in [CalTopo](https://caltopo.com).
A K9's Tractive collar only shows position inside the Tractive app — visible to
the handler, invisible to the team. This bridge relays the collar's fixes to a
CalTopo live track, so the dog draws a real track on the map the team is
already using.

## The switch: Tractive LIVE mode

The bridge is **off by default** and posts nothing. Tractive's own LIVE
tracking is the on/off switch:

- **Start a deployment:** turn LIVE on in the Tractive app (or run
  `bridge.py --on`). The bridge notices within seconds and starts relaying
  every fix to CalTopo.
- **End a deployment:** turn LIVE off (or `bridge.py --off`). The feed goes
  silent.

This means the handler's existing muscle memory — opening Tractive on a
callout — is the control surface. No extra app, no dashboard, works at 3am.

Two protections are built in:

- The bridge re-asserts LIVE during a deployment (Tractive times it out after
  ~5 minutes otherwise), so a deployment started remotely stays on.
- A hard cap (`MAX_LIVE_SECONDS`, default 8h) turns LIVE off if someone
  forgets, because LIVE drains the collar battery in hours, not days.

## Every deployment is its own track

A CalTopo map that adds a live track stays subscribed to that call sign
forever — with a fixed call sign, the dog reappears live on last month's
incident map the next time you deploy (like an ADS-B plane that shows up
whenever it flies). So the bridge mints a **dated device id per deployment**:
when a deployment starts it appends the date, `{DEVICE_ID}-yymmdd`, e.g.
call sign `TEAM1-Dog-260804`. `bridge.py --on` and `--status` print the
day's call sign; that's what the team adds to the incident map.

Old maps' subscriptions point at an id that never reports again, so CalTopo
finalizes the live track into a plain line object ~24h after its last fix —
a permanent record of that search, and nothing more ever draws on it. The
date is taken in `BRIDGE_TZ` (default `America/Los_Angeles`) so the call
sign matches the operational day, not UTC.

## How it works

```
Tractive collar -> Tractive event channel (push) -> bridge -> CalTopo live track
```

- Holds Tractive's real-time channel open via
  [aiotractive](https://github.com/zhulik/aiotractive) (the reverse-engineered
  app API — Tractive publishes no official one). Positions and LIVE-state
  changes arrive pushed; there is no polling loop.
- Posts to CalTopo's position-report endpoint
  (`https://caltopo.com/api/v1/position/report/{GROUP}?id={DEVICE}&lat=..&lng=..`)
  only when the fix timestamp changes — no duplicate points.

## CalTopo setup (once per map)

In CalTopo: map → add **live track** → Track Details → Type
`Fleet, Email, Other`. The **Call Sign** is the routing key — CalTopo splits it
at the first hyphen as `{GROUP}-{DEVICE}` (e.g. `TEAM1-Dog-260804`, where the
device id is `Dog-260804`). Use the day's call sign printed by `--on` /
`--status`. The **Label** is what shows on the map, independent of the call
sign.

Notes learned in the field:

- Save the track **before** positions arrive — earlier reports don't bind, and
  tracks do **not** backfill. Add the track early in an incident.
- Any team member can add the call sign to their own map; the bridge doesn't
  know or care how many maps subscribe.
- The endpoint has no authentication — the call sign is the only routing
  secret. Off-by-default means it only matters during a deployment, but still
  don't publish your real one.

## Running

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env    # fill in credentials + call sign
.venv/bin/python bridge.py            # the relay worker (deploy this)
.venv/bin/python bridge.py --status   # LIVE state + last fix
.venv/bin/python bridge.py --on       # start a deployment
.venv/bin/python bridge.py --off      # end a deployment
.venv/bin/python bridge.py --once     # post one fix (test the CalTopo side)
```

Credentials are the Tractive app login — keep them in `.env` (gitignored)
locally or your host's environment-variable settings in production. Never
commit them.

### Deploying (Sevalla or any worker host)

The worker is a plain long-lived process — e.g. a
[Sevalla](https://sevalla.com) **Background Worker** deployed from this repo
(the `Procfile` provides the start command; Nixpacks needs it). Set the
`.env.example` variables in the host's environment UI. The worker itself is
always running; whether it *reports* is governed entirely by LIVE mode.

## License

MIT
