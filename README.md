# Cable Watch: dark vessels over undersea infrastructure

Live ship positions from the Baltic stream through Confluent Cloud. Flink SQL
raises an incident when a vessel loiters or drags anchor over a subsea cable,
or switches off its AIS transponder nearby, and a fully managed HTTP Sink
connector sends the alert. No human in the loop between the ship's radio and
the notification on your phone.

See [IDEA.md](IDEA.md) for the background (Balticconnector, C-Lion1, Estlink 2).

```
aisstream.io (WebSocket)                          OpenStreetMap cable routes
        │                                                     │
  producer.py (Avro, H3 cell,                       tools/build_cables.py → loader.py
  data contract → DLQ)                                        │
        ▼                                                     ▼
 [ais_position_raw] ──► ais_position_dlq            [cable_cells] (compacted, H3 → cables)
        │
 ┌──────┴──────────────────── Confluent Cloud Flink SQL ─────────────────────┐
 │ 20 clean + dedupe ─► [vessel_fixes] ──────────────────────► Tableflow      │
 │ 21 temporal join on H3 cell ─► [corridor_fixes]                            │
 │ 22 TUMBLE per vessel+cable, slow fixes ─► [loiter_events]                  │
 │ 23 MATCH_RECOGNIZE anchor drag ─► [drag_events]                            │
 │ 24 SESSION gap per vessel + temporal join ─► [dark_events]                 │
 │ 25 union + points ─► [risk_signals]                                        │
 │ 26 OVER (1 h) running score / 27 SESSION (1 h) decay ─► [risk_updates]     │
 │ 28 upsert ─► [vessel_risk] ───────────────────────────────► Tableflow      │
 │ 29 LAG transitions + vessel_static join ─► [incidents] ───► Tableflow      │
 │ 32 ML_PREDICT brief (optional) ─► [incident_briefs]                        │
 │ 40-42 JSON bodies ─► [ntfy_events] [pagerduty_events] [slack_events]       │
 └───────────────────────────────────────────────────────────────────────────┘
        │                    │                       │
 HTTP Sink V2 ─► ntfy.sh   HTTP Sink V2 ─► PagerDuty   HTTP Sink V2 ─► Slack
        │
 FastAPI (SSE) ─► MapLibre dashboard
```

## Run it

You need a Confluent Cloud resource-management API key, a free
[aisstream.io](https://aisstream.io) key, AWS credentials, Terraform and Python 3.11+.

```sh
cp .env.example .env            # fill in the keys
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # optional
make infra                      # Confluent + AWS, prints the dashboard and ntfy URLs
```

`make infra` builds the environment (Stream Governance Advanced), a Basic
cluster, the raw topic with its data contract, every Flink statement, the HTTP
Sink connector(s), Tableflow and Stream Catalog tags. On AWS it builds one
`t4g.small` instance behind CloudFront that loads the cable cells, runs the
producer and serves the dashboard.

Open the ntfy URL (or scan the QR code on the dashboard) and press
**Replay the Eagle S incident**. A synthetic tanker crosses Estlink 2 in real
time and drags its anchor. Expect WATCH after about 4 minutes and an ALERT, with
a push notification, a minute or two later. 30 minutes after it falls silent a
dark-vessel signal follows, and an hour after that the incident resolves itself.

| Command | |
| --- | --- |
| `make status` | Phase of every Flink statement |
| `make env` | Write `app/.env.app` from the Terraform outputs |
| `make producer` / `make dashboard` / `make replay` | Run the parts locally against the same cluster |
| `make cables` | Rebuild `app/data` from OpenStreetMap |
| `make destroy` | Tear everything down |

### Optional outcomes

Add any of these to `.env` and run `make infra` again:

- `OPENAI_API_KEY`: a Flink AI connection and model; `ML_PREDICT` rewrites each
  incident into a two-sentence brief. Without it the brief is templated in SQL.
- `PAGERDUTY_ROUTING_KEY`: a second HTTP Sink to the PagerDuty Events API v2.
  Trigger and resolve share a `dedup_key`, so incidents close themselves.
  PagerDuty only accepts the key in the request body, so it is part of the
  `pagerduty_events` records.
- `SLACK_WEBHOOK_URL`: a third HTTP Sink posting a Block Kit message.

## How the detection works

- **Geofence as an equi-join.** The producer tags each fix with its H3
  resolution-7 cell. `cable_cells` maps every offshore cell within 10 km of a
  cable to the cables near it and their distances. One event-time temporal join
  replaces any point-in-polygon work. Cells within 3 km of land are left out,
  so ships alongside in a port where a cable lands are not counted as loitering.
- **Loitering** is a 20-minute tumbling window per vessel and cable over fixes
  under 2 knots. A session window would only close once the ship left, so a
  vessel parked on a cable for hours would raise nothing.
- **Anchor drag** is `MATCH_RECOGNIZE`: eight consecutive corridor fixes at
  1.5 to 7 knots, either reporting "at anchor" or moving more than 45° off the bow.
- **Going dark** is a 30-minute session window per vessel over every fix. The
  session closes when the watermark passes the gap, and the watermark is driven by
  every other ship in the Baltic, so a silent vessel still gets a clock.
  Fast ships are skipped: they are usually just sailing out of receiver range.
- **Score.** Loiter 20, drag 50, dark 40 points. The score is the sum over the
  last hour (OVER window): WATCH at 30, ALERT at 60. A one-hour session window
  over the signals emits the CLEAR that resolves the incident.
- **Heartbeat.** Signals are rare, and an event-time operator only emits once a
  later row moves its watermark forward. Idle-partition detection does not help
  when the whole topic is quiet. So `risk_signals` also carries one heartbeat row
  every 30 seconds, made by a tumbling window over the always-busy
  `vessel_fixes`. It is the ISS demo's idle-partition problem from the other side.
- **Transitions** use a two-row `ROWS` frame (`FIRST_VALUE ... ROWS BETWEEN 1
  PRECEDING AND CURRENT ROW`) instead of `LAG`, whose RANGE frame makes two
  updates with the same timestamp see each other.

### Operating notes

- Replacing a statement (changing its SQL) starts it again from the earliest
  offsets of its inputs, so downstream topics see that history a second time.
  Incident transitions are robust to the duplicates, but ntfy may repeat a
  notification.
- The dashboard reads Flink output with `isolation.level=read_uncommitted`.
  Flink commits its transactions at each checkpoint, and a map can draw rows as
  they are written instead of once a minute.

## Data and licences

- AIS: [aisstream.io](https://aisstream.io).
- Cable and pipeline routes: © OpenStreetMap contributors,
  [ODbL](https://www.openstreetmap.org/copyright). Land mask: Natural Earth (public domain).
- The replay track is synthetic, loosely based on public reporting about the
  Eagle S. It is labelled `[REPLAY]` everywhere.

## Differences from IDEA.md

- **Alerting goes to ntfy.sh by default.** It needs no account, and an audience can subscribe from the QR code.
- **`cable_cells` is keyed by H3 cell, with an array of cables.** A temporal join must cover the versioned table's whole primary key.
- **Loitering uses tumbling windows instead of a session window**, for the reason given under "How the detection works".
- **`vessel_risk` is keyed by vessel and cable**, so each cable gets its own incident.
- **Not built (yet):**
  - Watch-list scoring by flag or ship type. It scores the ship, not its behaviour.
  - The coverage-hole table for dark events.
  - Replay from Tableflow. The replay is generated rather than read back from Iceberg.
- **Basic cluster instead of Standard.** Data quality rules depend on the governance package, not on the cluster type.
