# Cable Watch: dark vessels over undersea infrastructure

Live ship positions from the Baltic stream through Confluent Cloud. Flink SQL
raises an incident when a vessel loiters or drags anchor over a subsea cable,
or switches off its AIS transponder nearby, and a fully managed HTTP Sink
connector sends the alert. No human in the loop between the ship's radio and
the notification on your phone.

## Why this demo

The Baltic Sea has had a run of subsea cable and pipeline damage:
Balticconnector (October 2023), C-Lion1 and BCS East-West (November 2024), and
Estlink 2 (December 2024). In each case the suspect vessel's behaviour was
visible afterwards in public AIS data: slow speed over the corridor, an anchor
dragged for tens of kilometres, gaps in the track.

None of these signals is in a single position report. They only exist across
time, per vessel, relative to a fixed geography. That is what stateful stream
processing is for, and it makes Confluent the whole product: ingest, contract,
enrich, detect, act and archive, all inside one platform. The only code outside
Confluent is the producer and the web page.

| Requirement | How Cable Watch meets it |
| --- | --- |
| Continuous stream | aisstream.io WebSocket over the Baltic: about 12 reports a second from roughly 3,000 vessels |
| Stateful logic | Flink SQL: temporal geofence join, tumbling, session and OVER windows, `MATCH_RECOGNIZE`, interval join |
| Automated outcome | Fully managed HTTP Sink V2 connector sends push notifications (ntfy.sh), optionally PagerDuty and Slack |
| Demo UI | Nautical-chart map: magenta cable symbols that turn amber and pulse red, ghost markers for dark vessels, live incident log |

## Architecture

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
 │ 24 SESSION gap + interval join + temporal join ─► [dark_events]            │
 │ 25 union + points + heartbeat ─► [risk_signals]                            │
 │ 26 OVER (1 h) running score ∪ SESSION (1 h) decay ─► [risk_updates]        │
 │ 28 upsert ─► [vessel_risk] ───────────────────────────────► Tableflow      │
 │ 29 state transitions + vessel_static join ─► [incidents] ─► Tableflow      │
 │ 32 ML_PREDICT brief (optional) ─► [incident_briefs]                        │
 │ 40-42 JSON bodies ─► [ntfy_events] [pagerduty_events] [slack_events]       │
 └───────────────────────────────────────────────────────────────────────────┘
        │                    │                       │
 HTTP Sink V2 ─► ntfy.sh   HTTP Sink V2 ─► PagerDuty   HTTP Sink V2 ─► Slack
        │
 FastAPI (SSE) ─► MapLibre dashboard
```

The numbers are the files in [`flink/`](flink). Terraform submits every one of
them as a `confluent_flink_statement`.

| Confluent component | What it shows |
| --- | --- |
| Kafka | Every stage is a topic; the pipeline strip on the dashboard shows each one's rate |
| Schema Registry (Avro) | Flink's `CREATE TABLE` registers the schemas; the producer and loader write with them and never register their own |
| Data contracts | CEL rules on `ais_position_raw` send impossible fixes to a DLQ, from inside the producer's serializer |
| Flink SQL | All detection, scoring and payload building, as declarative SQL |
| Flink AI (`ML_PREDICT`) | Optional: turns an incident into a two-sentence brief for the pager |
| HTTP Sink V2 connector | The side effect. No custom consumer touches the notification |
| Tableflow | `vessel_fixes`, `vessel_risk` and `incidents` as Iceberg tables |
| Stream Lineage | The live map from raw AIS to a notification; the dashboard's pipeline strip links to it |
| Stream Catalog tags | `geo`, `alerting` and `pii_free` on the topics |
| Terraform (`confluentinc/confluent`) | One `make infra` builds all of it, plus the AWS host |

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

Open the ntfy URL, or scan the QR code on the dashboard, to get the incidents
on your phone. In its first 17 hours on live traffic the pipeline raised about
100 WATCH incidents and a handful of ALERTs, mostly vessels falling silent near
the Gulf of Finland cables and Nord Stream. A notification arrives a few minutes
after the behaviour, because of the lateness allowances described below. An
incident resolves itself an hour after the vessel's last signal.

| Command | |
| --- | --- |
| `make status` | Phase of every Flink statement |
| `make env` | Write `app/.env.app` from the Terraform outputs |
| `make producer` / `make dashboard` | Run the parts locally against the same cluster |
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

## Data

### AIS positions

- `wss://stream.aisstream.io/v0/stream`, bounding box `[[53.5, 9.5], [66.0, 30.5]]`,
  message types `PositionReport`, `StandardClassBPositionReport` and `ShipStaticData`.
- Positions go to `ais_position_raw`, tagged by the producer with their H3
  resolution-7 cell. Static data (name, type, IMO, destination, dimensions) goes
  to the compacted `vessel_static`, which the incident statement joins as a
  versioned table.
- Coverage is terrestrial and patchy: dense in the Gulf of Finland and the Danish
  straits, sparse in the central Baltic.

### Cable corridors

[`tools/build_cables.py`](tools/build_cables.py) takes every named submarine
cable and pipeline in the box from OpenStreetMap: 47 assets (24 telecom, 19
power, 4 gas), including C-Lion1, Estlink 1 and 2, Balticconnector, BCS
East-West, NordBalt and Nord Stream. It buffers each route by 10 km and fills
the buffer with H3 resolution-7 cells (about 1.2 km edge), recording each cell's
distance to the route. Cells within 3 km of land are dropped. That leaves 27,131
cells, each listing every asset near it. The output is committed in `app/data`,
so a deploy never depends on Overpass being up.

**[CABLES.md](CABLES.md)** lists every asset with its operator, length and
corridor size. It also covers the incidents that motivated the demo, the
limitations of the OSM data, and how to change the set.

### Data contract

`ais_position_raw` is the one topic Terraform creates rather than Flink: its
schema carries Schema Registry domain rules (CEL), with action `DLQ`:

```
message.lat >= -90.0 && message.lat <= 90.0
message.lon >= -180.0 && message.lon <= 180.0
message.mmsi >= 100000000 && message.mmsi <= 999999999
message.sog >= 0.0 && message.sog < 102.2
```

AIS uses 91 latitude, 181 longitude and 102.3 knots for "not available". The
Python client (confluent-kafka 2.16) runs the rules in the serializer and writes
failures to `ais_position_dlq` with `__rule.*` headers naming the broken rule.

### Topics

| Topic | Key | Notes |
| --- | --- | --- |
| `ais_position_raw` | MMSI | Producer output, Avro, 24 h |
| `ais_position_dlq` | MMSI | Rejected by the data contract, JSON, 7 d |
| `vessel_static` | MMSI | Compacted: name, type, IMO, dimensions |
| `cable_cells` | H3 cell | Compacted: cables within 10 km of the cell and their distances |
| `vessel_fixes` | MMSI | Deduplicated, cleaned, event time; Tableflow |
| `corridor_fixes` | MMSI | Fixes within 2 km of a cable, once per cable |
| `loiter_events` / `drag_events` / `dark_events` | MMSI | One row per detection, 30 d |
| `risk_signals` | MMSI, cable | The detections with points, plus heartbeats |
| `risk_updates` | MMSI, cable | Every score change |
| `vessel_risk` | MMSI, cable | Compacted: latest score and state; Tableflow |
| `incidents` | MMSI, cable | WATCH, TRIGGER and RESOLVE transitions with a brief; Tableflow |
| `ntfy_events` / `pagerduty_events` / `slack_events` | none | Exact HTTP bodies for the sink connectors |

## How the detection works

- **Clean and dedupe.** Base stations relay the same report, so `ROW_NUMBER()`
  keeps the first copy of each (vessel, AIS second, position). "Not available"
  heading and course become NULL.
- **Geofence as an equi-join.** One event-time temporal join on the H3 cell
  replaces any point-in-polygon work. `cable_cells` is keyed by the cell alone,
  because a temporal join must cover the versioned table's whole key.
- **Loitering** is a 20-minute tumbling window per vessel and cable over fixes
  under 2 knots. A session window would only close once the ship left, so a
  vessel parked on a cable for hours would raise nothing.
- **Anchor drag** is `MATCH_RECOGNIZE`: eight consecutive corridor fixes at
  1.5 to 7 knots, either reporting "at anchor" or moving more than 45° off the
  bow. That is the Eagle S signature.
- **Going dark** is a 30-minute session window per vessel over every fix. The
  session closes when the watermark passes the gap, and the watermark is driven
  by every other ship in the Baltic, so a silent vessel still gets a clock. An
  interval join fetches the session's last fix, and a temporal join checks
  whether it was within 10 km of a cable. Fast ships are skipped: they are
  usually just sailing out of receiver range.
- **Score.** The score is the sum of points over the last hour (OVER window). A
  one-hour session window over the signals emits the CLEAR that resolves the
  incident.

  | Signal | Points |
  | --- | --- |
  | Loitering over a corridor, per 20 minutes | 20 |
  | Anchor drag over a corridor | 50 |
  | Dark within 10 km of a cable | 40 |

  NORMAL under 30, WATCH 30 to 59, ALERT 60 and over.
- **Incidents** are state transitions per vessel and cable, enriched with
  `vessel_static`. The payload statements turn them into the exact JSON each
  HTTP Sink posts. An ntfy notification opens the vessel on the dashboard.

### Lessons from running it on live data

Every one of these looked fine in a quick test and broke on the real feed.

- **Static reference data needs a far-future watermark.** A temporal join waits
  for the slower of its two inputs. `cable_cells` is written once with 2020
  timestamps, and every reload pinned the join to 2020: the whole pipeline
  downstream froze, and idleness detection did not always rescue it.
  `00_alter_cable_cells.sql` sets its watermark a century ahead, so only
  `vessel_fixes` drives the clock. That is only safe with idleness detection
  off for the joining statements (`sql.tables.scan.idle-timeout = 0 ms`). The
  first rebuild left it on: `vessel_fixes` was idle before the producer
  started, the join's watermark jumped to the year 2119, and every fix after
  that was dropped as late. The loader also skips the load when the topic
  already has data.
- **Sparse topics need a heartbeat.** Signals are rare, and an event-time
  operator only emits once a later row moves its watermark forward.
  Idle-partition detection does not help when the whole topic is quiet. So
  `risk_signals` also carries one heartbeat row every 30 seconds, made by a
  tumbling window over the always-busy `vessel_fixes`.
- **Checkpoints make output bursty.** Flink commits its Kafka transactions once
  per checkpoint, about a minute. A keyed operator emits each burst in key
  order, not time order, so rows land up to a minute "late" against each other.
  `risk_signals` allows 3 minutes of lateness and `risk_updates` 2. These
  allowances are most of the delay between a detection and its notification.
- **One writer per topic.** The running score and the decay used to be two
  statements writing `risk_updates`. Their transactions committed at different
  checkpoints, their rows interleaved out of event-time order, and the incident
  statement dropped the "late" ones. They now run as one statement (`UNION ALL`).
- **`LAG` is unsafe with ties.** It uses a RANGE frame, where two updates with the
  same timestamp see each other. Transitions use a two-row `ROWS` frame
  (`FIRST_VALUE ... ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`) instead.
- **`LAST_VALUE` is not allowed in session windows** on Confluent Cloud, which is
  why the dark detector uses an interval join to fetch the last fix.

### Operating notes

- Replacing a statement (changing its SQL) starts it again from the earliest
  offsets of its inputs, so downstream topics see that history a second time.
  Transitions are robust to the duplicates, and the payload statements never
  send an incident older than 30 minutes, so nobody is paged for history.
- The dashboard reads Flink output with `isolation.level=read_uncommitted`, so
  the map draws rows as they are written instead of once a minute.
- `scripts/flink-status.sh` (`make status`) lists every statement's phase. The
  Confluent Metrics API (`io.confluent.flink/current_input_watermark_milliseconds`,
  grouped by statement) is the quickest way to find a stalled one.

## Not built yet

- **Watch-list scoring by flag or ship type.** It would score the ship, not its behaviour, which makes the demo look like profiling.
- **A coverage-hole table** to down-weight dark events where receivers are sparse.
- **A replay mode.** An earlier version replayed a synthetic Eagle S track on demand. It was removed because anyone with the dashboard URL could send notifications to every subscriber.

## Data and licences

- AIS: [aisstream.io](https://aisstream.io).
- Cable and pipeline routes: © OpenStreetMap contributors,
  [ODbL](https://www.openstreetmap.org/copyright). Land mask: Natural Earth (public domain).
- Base map: © CARTO, © OpenStreetMap contributors.
