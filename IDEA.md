# Cable Watch: Dark Vessels Over Undersea Infrastructure

> Live ship positions stream through Confluent Cloud. Flink SQL raises an
> incident when a vessel loiters or drags anchor over a subsea cable, or
> switches off its AIS transponder nearby, and a fully managed connector pages
> someone. No human in the loop between the ship's radio and the pager.

## Why this demo

The Baltic Sea has had a run of subsea cable and pipeline damage:
Balticconnector (October 2023), C-Lion1 and BCS East-West (November 2024), and
Estlink 2 (December 2024). In each case the suspect vessel's behaviour was
visible afterwards in public AIS data: slow speed over the corridor, an anchor
dragged for tens of kilometres, gaps in the track.

None of these signals is in a single position report. They only exist across
time, per vessel, relative to a fixed geography. That is exactly what stateful
stream processing is for, and it makes Confluent the whole product: ingest,
contract, enrich, detect, act, and archive, all inside one platform.

## The four requirements

| Requirement | How Cable Watch meets it |
| --- | --- |
| Continuous stream | aisstream.io WebSocket, Baltic bounding box, tens to hundreds of reports a second |
| Stateful logic | Flink SQL: temporal geofence join, session windows, `MATCH_RECOGNIZE`, per-vessel risk score |
| Automated outcome | Fully managed HTTP Sink connector triggers and resolves PagerDuty incidents, posts to Slack |
| Demo UI | Dark nautical map, pulsing cable corridors, ghost markers for dark vessels, live incident cards |

## Architecture

```
aisstream.io (WebSocket)                         TeleGeography / OSM cable GeoJSON
        │                                                    │
  Python producer (Avro, H3 cell)                    one-off loader (Avro)
        │                                                    │
        ▼                                                    ▼
 [ais_position_raw] ── data contract ──► DLQ        [cable_cells] (compacted)
        │
        ▼
 ┌──────────────────────── Confluent Cloud Flink SQL ────────────────────────┐
 │ clean + dedupe ─► [vessel_fixes]                                          │
 │ temporal join on H3 cell ─► [corridor_fixes]                              │
 │ SESSION window per MMSI ─► [loiter_events]                                │
 │ MATCH_RECOGNIZE (anchor drag) ─► [drag_events]                            │
 │ SESSION gap per MMSI ─► [dark_events]                                     │
 │ risk score + state ─► [vessel_risk]                                       │
 │ ML_PREDICT incident brief ─► [incident_briefs]                            │
 │ PagerDuty payload builder ─► [pagerduty_events] / [slack_events]          │
 └───────────────────────────────────────────────────────────────────────────┘
        │                         │                          │
        ▼                         ▼                          ▼
 HTTP Sink V2 ─► PagerDuty   HTTP Sink V2 ─► Slack    Tableflow ─► Iceberg (S3)
        │
        ▼
 FastAPI (SSE) ─► MapLibre + deck.gl dashboard
```

## Confluent components, and what each one proves

| Component | Role in the demo |
| --- | --- |
| Confluent Cloud Kafka | Every stage is a topic. The pipeline strip on the page shows each one's rate |
| Schema Registry (Avro) | `CREATE TABLE` in Flink registers schemas. The producer fetches them, as in the ISS demo |
| Data contracts (Data Quality Rules) | CEL rules reject impossible fixes into a DLQ before Flink ever sees them |
| Flink SQL | All detection logic, as declarative SQL files submitted by Terraform |
| Flink AI (`CREATE MODEL`, `ML_PREDICT`) | Turns a raw risk event into a two-sentence incident brief for the pager |
| Fully managed HTTP Sink V2 connector | The side effect. PagerDuty and Slack calls happen with no custom consumer |
| Tableflow | Materialises `vessel_fixes` and `vessel_risk` as Iceberg tables for replay and analysis |
| Stream Lineage | The live map of the pipeline, from raw AIS to a PagerDuty incident, for the "how it works" slide |
| Stream Catalog tags | Tags `PII`-free, `geo`, `alerting` topics so the governance story is visible |
| Terraform provider (`confluentinc/confluent`) | One `terraform apply` builds the environment, cluster, schemas, statements, connectors, and Tableflow |

## Data sources

### AIS positions

- **Endpoint:** `wss://stream.aisstream.io/v0/stream` (free API key).
- **Subscription:** bounding box over the Baltic, roughly `[[53.5, 9.5], [66.0, 30.5]]`,
  `FilterMessageTypes: ["PositionReport", "ShipStaticData"]`.
- **Fields used:**
  - `PositionReport`: `UserID` (MMSI), `Latitude`, `Longitude`, `Sog`, `Cog`,
    `TrueHeading`, `NavigationalStatus`, `MetaData.time_utc`.
  - `ShipStaticData`: `Name`, `Type`, `Destination`, `Dimension`, `ImoNumber`.
- `ShipStaticData` goes to its own compacted topic, `vessel_static`, keyed by
  MMSI, and is joined in as a versioned table.

### Cable corridors

- Cable routes for the Baltic from a public GeoJSON (TeleGeography's submarine
  cable map or OpenStreetMap `power=cable` / `telecom=cable` with
  `location=underwater`). Check the licence before redistributing.
- A one-off loader buffers each route by 2 km and fills the buffer with H3
  cells at resolution 7 (about 1.2 km edge). Each cell is written to
  `cable_cells` as `{h3_r7, cable_id, cable_name, operator}`.
- A cell can sit over more than one cable, so the key is `h3_r7 + cable_id`.

## Topics and schemas

| Topic | Key | Cleanup | Notes |
| --- | --- | --- | --- |
| `ais_position_raw` | MMSI | delete, 24 h | Producer output, Avro, carries `h3_r7` computed in Python |
| `ais_position_dlq` | MMSI | delete, 7 d | Rejected by data contract rules |
| `vessel_static` | MMSI | compact | Name, type, IMO, dimensions |
| `cable_cells` | `h3_r7`, `cable_id` | compact | Static geography |
| `vessel_fixes` | MMSI | delete, 7 d | Deduplicated, typed, event-time |
| `corridor_fixes` | MMSI | delete, 7 d | Fixes that fall inside a cable buffer |
| `loiter_events` / `drag_events` / `dark_events` | MMSI | delete, 30 d | One row per detection |
| `vessel_risk` | MMSI | compact | Latest score and state per vessel |
| `incident_briefs` | `mmsi + cable_id` | delete, 30 d | LLM output |
| `pagerduty_events` / `slack_events` | `mmsi + cable_id` | delete, 7 d | Exact HTTP bodies for the sink connectors |

### Data contract rules on `ais_position_raw`

Schema Registry domain rules (CEL), action `DLQ`:

```
message.latitude >= -90.0 && message.latitude <= 90.0
message.longitude >= -180.0 && message.longitude <= 180.0
message.mmsi >= 100000000 && message.mmsi <= 999999999
message.sog >= 0.0 && message.sog < 102.3
```

AIS uses `91` latitude, `181` longitude, and `102.3` knots as "not available".
Real feeds carry them constantly, so the DLQ rate is a nice live number on the
pipeline strip.

## Flink SQL statements

All tables use `time_utc` as event time with a short watermark (for example
`time_utc - INTERVAL '10' SECOND`). Statement files live in `flink/` and are
submitted by Terraform with `confluent_flink_statement`.

### 1. Clean and dedupe

`ROW_NUMBER() OVER (PARTITION BY mmsi, time_utc ORDER BY $rowtime)` keeps the
first copy of each report. Base stations relay the same message, so duplicates
are common.

### 2. Geofence: temporal join on H3

```sql
INSERT INTO corridor_fixes
SELECT f.mmsi, f.time_utc, f.lat, f.lon, f.sog, f.cog, f.heading, f.nav_status,
       c.cable_id, c.cable_name
FROM vessel_fixes AS f
JOIN cable_cells FOR SYSTEM_TIME AS OF f.time_utc AS c
  ON f.h3_r7 = c.h3_r7;
```

Computing the H3 cell in the producer keeps Flink to an equi-join. A Java UDF
(`H3_LATLNG_TO_CELL`) on Confluent Cloud is the alternative if the producer
should stay dumb.

### 3. Loitering: session windows per vessel and cable

```sql
INSERT INTO loiter_events
SELECT mmsi, cable_id, window_start, window_end,
       COUNT(*) AS fixes, MAX(sog) AS max_sog
FROM TABLE(
  SESSION(TABLE corridor_fixes PARTITION BY mmsi, cable_id,
          DESCRIPTOR(time_utc), INTERVAL '5' MINUTES))
WHERE sog < 2.0
GROUP BY mmsi, cable_id, window_start, window_end
HAVING window_end - window_start > INTERVAL '20' MINUTES;
```

Filter on slow fixes before the window, so a session only covers time spent
nearly stationary over the corridor.

### 4. Anchor drag: `MATCH_RECOGNIZE`

```sql
SELECT *
FROM corridor_fixes
MATCH_RECOGNIZE (
  PARTITION BY mmsi
  ORDER BY time_utc
  MEASURES FIRST(D.time_utc) AS drag_start, LAST(D.time_utc) AS drag_end,
           FIRST(D.cable_id) AS cable_id, AVG(D.sog) AS avg_sog,
           COUNT(D.mmsi) AS fixes
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (D{6,}) WITHIN INTERVAL '2' HOUR
  DEFINE D AS (D.sog BETWEEN 2.0 AND 6.0 AND D.nav_status = 1)
           OR (D.sog BETWEEN 2.0 AND 6.0
               AND ABS(MOD(D.heading - D.cog + 540, 360) - 180) > 45)
);
```

Status `1` is "at anchor". A ship moving at several knots while reporting
anchored, or moving sideways relative to its heading, is the Eagle S signature.

### 5. Going dark: session gaps

A `SESSION` window over all of `vessel_fixes` per MMSI with a 30-minute gap
closes when a vessel stops transmitting. If the session's last fix was within
10 km of a cable (join the last cell against a wider `cable_cells_10km` set),
emit a `dark_events` row with the last position.

A session closes when the watermark passes the gap, and the watermark is
driven by every other ship in the box, so a silent vessel still gets a clock.
That is the opposite of the idle-partition problem in the ISS demo, and worth
a line in the write-up.

Terrestrial AIS coverage has holes. A small `coverage_cells` table (cells where
the median gap is already high) down-weights dark events there.

### 6. Risk score and state

A statement unions the three event streams and keeps a running score per
vessel in `vessel_risk` (upsert, keyed by MMSI):

| Signal | Points |
| --- | --- |
| Loitering over a corridor, per 20 minutes | 20 |
| Anchor drag over a corridor | 50 |
| Dark within 10 km of a cable | 40 |
| Flag or type on a watch list (from `vessel_static`) | 15 |

States: `NORMAL` under 30, `WATCH` 30 to 59, `ALERT` 60 and over. The score
decays when the vessel has been outside every corridor for an hour.

### 7. Incident brief: Flink AI

```sql
CREATE MODEL incident_writer
INPUT (prompt STRING) OUTPUT (brief STRING)
WITH ('provider' = 'openai', 'task' = 'text_generation',
      'openai.connection' = 'openai-conn',
      'openai.system_prompt' = 'You write two-sentence maritime incident briefs for an on-call engineer. Facts only.');

INSERT INTO incident_briefs
SELECT r.mmsi, r.cable_id, p.brief
FROM vessel_risk_alerts AS r,
LATERAL TABLE(ML_PREDICT('incident_writer', r.prompt)) AS p;
```

`prompt` is built in SQL from vessel name, type, cable name, speed history, and
which signals fired. Bedrock or Azure OpenAI work the same way through a
different connection.

### 8. Payload builder

The last statement shapes the exact JSON bodies for the sink connectors:

```json
{
  "routing_key": "<from connector config>",
  "event_action": "trigger",
  "dedup_key": "cablewatch-<mmsi>-<cable_id>",
  "payload": {
    "summary": "EAGLE S dragging anchor over Estlink 2",
    "source": "cablewatch-flink",
    "severity": "critical",
    "custom_details": { "brief": "...", "score": 85, "lat": 59.9, "lon": 25.2 }
  },
  "links": [{ "href": "https://<dashboard>/vessel/<mmsi>", "text": "Live track" }]
}
```

When a vessel drops back to `NORMAL`, the same statement emits
`event_action: "resolve"` with the same `dedup_key`, and the incident closes
itself.

## The automated outcome

Two fully managed **HTTP Sink V2** connectors, created by Terraform:

- `pagerduty-sink` reads `pagerduty_events` and posts each record to
  `https://events.pagerduty.com/v2/enqueue`. The routing key is a connector
  secret, injected into the body, so it never sits in a topic.
- `slack-sink` reads `slack_events` and posts to a Slack incoming webhook with
  a Block Kit message (vessel, cable, score, brief, map link).
- Both connectors send failed calls to their own DLQ topic, shown on the page.

No custom consumer touches the side effect. The demo line is: "the only code
outside Confluent is the producer and the web page."

## Dashboard

- **Map:** dark nautical basemap (MapLibre). Cable corridors as glowing lines
  that turn amber at `WATCH` and pulse red at `ALERT`. Ship icons rotated by
  heading and coloured by score. A dark vessel leaves a hollow "last seen"
  ghost with a timer counting up.
- **Vessel panel:** click a ship for its speed sparkline, nav status changes,
  corridor entries, and the signals that scored points.
- **Incident cards:** one per open PagerDuty incident, with the LLM brief and
  `TRIGGERED` / `RESOLVED` state mirrored from `pagerduty_events`.
- **Pipeline strip:** every topic with its live rate, plus the DLQ counts for
  the data contract and the connectors. Clicking it opens Stream Lineage.
- **Replay mode:** play back a recorded incident window from Tableflow at 20x,
  so the demo always has an alert to show even on a quiet day.

## Replay and the "always has an incident" problem

Real cable incidents are rare. The demo needs a guaranteed moment:

1. Tableflow keeps `vessel_fixes` as an Iceberg table in S3.
2. A `replay` producer reads a chosen window back from Iceberg (or a bundled
   Parquet file reconstructed from public reports of the Eagle S track) and
   writes it to `ais_position_raw` with a `replay = true` header and rebased
   timestamps.
3. The same Flink statements fire on it. PagerDuty gets a real incident,
   tagged `[REPLAY]` in the summary.

This doubles as the test harness for the detection SQL.

## Tech stack

- **Confluent Cloud:** Standard Kafka (Data Quality Rules need the Advanced
  Stream Governance package), Schema Registry, Flink compute pool, HTTP Sink
  V2 connectors, Tableflow, Stream Lineage.
- **Producer:** Python, `websockets`, `confluent-kafka` with the Avro
  serializer, `h3`.
- **Dashboard:** FastAPI with SSE, MapLibre GL, deck.gl.
- **Infrastructure:** Terraform with `confluentinc/confluent` and
  `hashicorp/aws`. One `t4g.small`, CloudFront, Secrets Manager, SSM, matching
  the ISS demo.
- **AI:** OpenAI or Bedrock connection for `ML_PREDICT`.

## Repo layout (proposed)

```
cableWatch/
├── IDEA.md
├── Makefile
├── terraform/
│   ├── confluent/      # env, cluster, SR, contracts, Flink, connectors, Tableflow
│   └── aws/            # EC2, CloudFront, S3, secrets
├── flink/              # one .sql file per statement, numbered
├── producer/           # aisstream -> ais_position_raw, vessel_static
├── loader/             # cable GeoJSON -> cable_cells
├── replay/             # Iceberg / Parquet -> ais_position_raw
└── app/                # FastAPI + static dashboard
```

## Milestones

1. Producer and topics: AIS flowing into Confluent Cloud with Avro and the
   data contract, DLQ rate visible.
2. Cable cells loaded, geofence join working, corridor fixes on a basic map.
3. Loitering, anchor drag, and dark detection, validated against the replay
   file.
4. Risk score, payload builder, HTTP Sink connectors to a PagerDuty trial
   service and Slack.
5. `ML_PREDICT` incident briefs.
6. Tableflow and replay mode.
7. Dashboard polish, Stream Lineage screenshot, portfolio project page.

## Open questions

- **Cable data licence:** TeleGeography's GeoJSON may not allow redistribution.
  OpenStreetMap is ODbL and safe to bundle with attribution.
- **Feature availability:** confirm `SESSION` windows, `ML_PREDICT`, HTTP Sink
  V2, and Tableflow are available in the chosen cloud region, and which
  Stream Governance package the contract rules need.
- **Volume:** measure the real Baltic message rate before sizing partitions.
  Keyed by MMSI, a handful of partitions is plenty, and every partition will
  see traffic, so idle watermarks should not be an issue.
- **Watch list:** decide whether flag or vessel-type scoring is in scope, or
  whether it makes the demo feel like it is profiling ships rather than
  behaviour.
