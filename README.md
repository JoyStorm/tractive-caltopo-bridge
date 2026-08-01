# tractive-caltopo-bridge

Put a dog's Tractive GPS collar on a live CalTopo map.

Search-and-rescue teams run their incident maps in [CalTopo](https://caltopo.com).
A K9's Tractive collar only shows position inside the Tractive app — visible to
the handler, invisible to the team. This bridge polls the collar through
Tractive's app API and reports each new fix to a CalTopo live track, so the dog
draws a real track on the same map the team is already using.

## How it works

```
Tractive collar  ->  Tractive cloud  ->  bridge (this repo)  ->  CalTopo live track
```

- Polls the collar's latest fix via [aiotractive](https://github.com/zhulik/aiotractive)
  (the reverse-engineered app API — Tractive publishes no official one).
- Posts to CalTopo's position-report endpoint:
  `https://caltopo.com/api/v1/position/report/{GROUP}?id={DEVICE}&lat=..&lng=..`
  (plain GET, no auth).
- Only posts when the fix **timestamp** changes — sparse collar updates never
  become duplicate points.

## CalTopo setup (once per map)

In CalTopo: map → add **live track** → Track Details → Type
`Fleet, Email, Other`. The **Call Sign** is the routing key — CalTopo splits it
at the first hyphen as `{GROUP}-{DEVICE}` (e.g. `54K9-Storm`). The **Label** is
what shows on the map, independent of the call sign.

Save the track **before** sending positions — reports fired before the track is
saved don't bind to it. The same call sign can be added to any number of maps;
the bridge doesn't know or care which maps subscribe.

## Idle vs. live

A Tractive collar normally reports minutes apart (stretching further when the
dog is still) to save battery. Polling faster than the collar reports just
returns the same stale fix.

- **idle** (default): slow poll, collar cadence untouched. Always-on safe.
- **live** (`--live` or `MODE=live`): enables Tractive LIVE tracking — a fix
  every few seconds, at the cost of collar battery measured in **hours, not
  days**. Use during a deployment, turn off after. The bridge disables LIVE
  mode on shutdown.

## Running

```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in credentials + call sign
.venv/bin/python bridge.py            # idle mode
.venv/bin/python bridge.py --live     # search mode
.venv/bin/python bridge.py --once     # single fix, then exit (smoke test)
```

Credentials are the Tractive app login — there are no API keys. Keep them in
`.env` (gitignored) locally, or in your host's environment-variable settings in
production. Never commit them.

### Deploying (Sevalla or any worker host)

Runs as a plain long-lived process — e.g. a
[Sevalla](https://sevalla.com) **Background Worker** deployed from this repo,
with the `.env.example` variables set in the host's environment UI.
Start command: `python bridge.py` (set `MODE=live` and restart for a search,
or run a second worker in live mode only during deployments).

## License

MIT
