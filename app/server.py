"""Cable Watch dashboard: FastAPI + server-sent events.

One background thread consumes the Flink output topics and keeps the current
picture in memory: vessel positions (vessel_fixes), risk per vessel and cable
(vessel_risk), detections (risk_signals, dark_events) and incidents. Another
samples every topic's end offsets for the pipeline strip. Browsers get a
snapshot on connect, then a delta every second.
"""
import asyncio
import io
import json
import os
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path

import qrcode
import qrcode.image.svg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

import config
import serde
import vessels as vessel_info

STATIC_DIR = Path(__file__).with_name("static")
TRACK_POINTS = 120
VESSEL_TTL_S = 3 * 3600       # drop vessels not heard from for this long
FIXES_LOOKBACK_S = 30 * 60    # on start, read this much vessel_fixes history
EVENTS_LOOKBACK_S = 24 * 3600
RISK_TTL_S = 65 * 60          # a risk state older than this is NORMAL by definition
DARK_MAX_SOG = 3.0            # same rule as 24_insert_dark_events.sql, for older events in the topic

# Pipeline strip, in data-flow order. Topics that don't exist (optional outcomes) are skipped.
PIPELINE = [
    ("ais_position_raw", "raw AIS"),
    ("ais_position_dlq", "contract DLQ"),
    ("vessel_static", "static"),
    ("vessel_fixes", "fixes"),
    ("corridor_fixes", "corridor"),
    ("loiter_events", "loiter"),
    ("drag_events", "drag"),
    ("dark_events", "dark"),
    ("risk_signals", "signals"),
    ("vessel_risk", "risk"),
    ("incidents", "incidents"),
    ("incident_briefs", "LLM briefs"),
    ("ntfy_events", "ntfy"),
    ("pagerduty_events", "PagerDuty"),
    ("slack_events", "Slack"),
    ("ntfy_events_dlq", "ntfy DLQ"),
    ("pagerduty_events_dlq", "PagerDuty DLQ"),
    ("slack_events_dlq", "Slack DLQ"),
]


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.seq = 0
        self.vessels: dict[int, dict] = {}
        self.risk: dict[str, dict] = {}          # "mmsi|cable_id" -> latest vessel_risk row
        self.signals: dict[int, deque] = {}      # mmsi -> recent risk_signals
        self.static: dict[int, dict] = {}        # mmsi -> latest vessel_static row
        self.dark: dict[int, dict] = {}          # mmsi -> latest dark_events row
        self.incidents: deque = deque(maxlen=200)
        self.rates: dict = {"topics": [], "t": 0}
        self.started = time.time()

    def bump(self) -> int:
        self.seq += 1
        return self.seq


state = State()


def iso(ts) -> str | None:
    return ts.isoformat() if ts is not None else None


def ms(ts) -> int | None:
    return int(ts.timestamp() * 1000) if ts is not None else None


# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------
def handle_fix(v: dict) -> None:
    mmsi = v["mmsi"]
    with state.lock:
        vessel = state.vessels.get(mmsi)
        if vessel is None:
            vessel = state.vessels[mmsi] = {
                "mmsi": mmsi, "track": deque(maxlen=TRACK_POINTS), "nav_changes": deque(maxlen=20),
            }
        t = ms(v["time_utc"])
        if vessel.get("t") and t < vessel["t"]:
            return
        if vessel.get("nav") is not None and vessel["nav"] != v["nav_status"]:
            vessel["nav_changes"].append({"t": t, "from": vessel["nav"], "to": v["nav_status"]})
        vessel.update({
            "lat": v["lat"], "lon": v["lon"], "sog": v["sog"], "cog": v["cog"], "hdg": v["heading"],
            "nav": v["nav_status"], "name": v["ship_name"] or vessel.get("name"), "t": t,
            "seq": state.bump(),
        })
        vessel["track"].append((round(v["lon"], 5), round(v["lat"], 5), round(v["sog"], 1), t))
        if mmsi in state.dark and state.dark[mmsi]["last_seen"] < t:
            state.dark.pop(mmsi)  # transmitting again


def handle_static(v: dict) -> None:
    with state.lock:
        state.static[v["mmsi"]] = v


def handle_corridor(v: dict) -> None:
    with state.lock:
        vessel = state.vessels.get(v["mmsi"])
        if vessel is not None:
            vessel["corridor_t"] = ms(v["time_utc"])
            vessel["seq"] = state.bump()


def handle_risk(v: dict) -> None:
    key = f"{v['mmsi']}|{v['cable_id']}"
    with state.lock:
        state.risk[key] = {
            "mmsi": v["mmsi"], "cable_id": v["cable_id"], "cable_name": v["cable_name"],
            "name": v["ship_name"], "score": v["score"], "state": v["state"], "signal": v["last_signal"],
            "loiter": v["loiter_count"], "drag": v["drag_count"], "dark": v["dark_count"],
            "lat": v["lat"], "lon": v["lon"], "t": ms(v["updated_at"]),
            "watch": v.get("watch_reason"), "watch_points": v.get("watch_points") or 0,
            "seq": state.bump(),
        }


def handle_signal(v: dict) -> None:
    with state.lock:
        state.signals.setdefault(v["mmsi"], deque(maxlen=30)).append({
            "t": ms(v["event_time"]), "signal": v["signal"], "points": v["points"],
            "cable_id": v["cable_id"], "cable_name": v["cable_name"], "sog": v["sog"],
            "distance_km": v["distance_km"],
        })
        if v["mmsi"] in state.vessels:
            state.vessels[v["mmsi"]]["seq"] = state.bump()


def handle_dark(v: dict) -> None:
    if (v.get("last_sog") or 0) >= DARK_MAX_SOG:
        return
    with state.lock:
        vessel = state.vessels.get(v["mmsi"])
        last_seen = ms(v["last_seen"])
        if vessel and vessel.get("t", 0) > last_seen:
            return  # it has transmitted since
        state.dark[v["mmsi"]] = {
            "mmsi": v["mmsi"], "name": v["ship_name"], "cable_id": v["cable_id"], "cable_name": v["cable_name"],
            "lat": v["lat"], "lon": v["lon"], "last_seen": last_seen, "distance_km": v["distance_km"],
            "seq": state.bump(),
        }


def handle_incident(v: dict) -> None:
    with state.lock:
        state.incidents.append({
            "mmsi": v["mmsi"], "cable_id": v["cable_id"], "t": ms(v["event_time"]), "action": v["action"],
            "state": v["state"], "score": v["score"], "signal": v["signal"], "cable_name": v["cable_name"],
            "name": v["vessel_name"], "type": v["vessel_type"], "lat": v["lat"], "lon": v["lon"],
            "summary": v["summary"], "brief": v["brief"], "seq": state.bump(),
        })


def handle_brief(v: dict) -> None:
    # LLM brief for an incident already shown: swap in the better text.
    with state.lock:
        for inc in reversed(state.incidents):
            if inc["mmsi"] == v["mmsi"] and inc["cable_id"] == v["cable_id"] and inc["t"] == ms(v["event_time"]):
                inc["brief"] = v["brief"]
                inc["llm"] = True
                inc["seq"] = state.bump()
                break


HANDLERS = {
    config.TOPIC_FIXES: (handle_fix, FIXES_LOOKBACK_S),
    "corridor_fixes": (handle_corridor, FIXES_LOOKBACK_S),
    config.TOPIC_STATIC: (handle_static, None),  # compacted: read it all
    config.TOPIC_RISK: (handle_risk, None),  # compacted: read it all
    config.TOPIC_SIGNALS: (handle_signal, EVENTS_LOOKBACK_S),
    config.TOPIC_DARK: (handle_dark, 3 * 3600),
    config.TOPIC_INCIDENTS: (handle_incident, EVENTS_LOOKBACK_S),
    "incident_briefs": (handle_brief, EVENTS_LOOKBACK_S),
}


def consume_forever() -> None:
    while True:
        try:
            consume()
        except Exception as exc:  # keep the dashboard up through broker hiccups
            print(f"consumer error: {exc!r}; restarting in 10 s", file=sys.stderr)
            time.sleep(10)


def consume() -> None:
    sr = serde.registry()
    consumer = Consumer({
        **config.kafka_conf(),
        "group.id": f"cablewatch-dashboard-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        # Flink writes transactionally and commits at each checkpoint; a map can
        # show its output as it is written rather than once a minute.
        "isolation.level": "read_uncommitted",
    })
    topics = consumer.list_topics(timeout=15).topics
    deserializers, assignments = {}, []
    now_ms = int(time.time() * 1000)
    for topic, (_, lookback) in HANDLERS.items():
        if topic not in topics:
            continue
        deserializers[topic] = AvroDeserializer(sr)
        partitions = [TopicPartition(topic, p, now_ms - lookback * 1000 if lookback else -2)
                      for p in topics[topic].partitions]
        if lookback:
            partitions = consumer.offsets_for_times(partitions, timeout=15)
            for tp in partitions:
                if tp.offset < 0:
                    tp.offset = -1  # nothing that recent: start at the end
        assignments.extend(partitions)
    consumer.assign(assignments)
    print(f"dashboard consuming {sorted(deserializers)}")
    while True:
        for msg in consumer.consume(500, timeout=1.0):
            if msg.error() or msg.value() is None:
                continue
            topic = msg.topic()
            try:
                value = deserializers[topic](msg.value(), SerializationContext(topic, MessageField.VALUE))
                HANDLERS[topic][0](value)
            except Exception as exc:
                print(f"{topic}: {exc!r}", file=sys.stderr)


def sample_rates_forever() -> None:
    consumer = Consumer({**config.kafka_conf(), "group.id": "cablewatch-rates", "enable.auto.commit": False})
    history: dict[str, deque] = {}  # topic -> (time, end offset) over the last minute
    topics: dict = {}
    last_list = 0.0
    while True:
        try:
            if time.time() - last_list > 60:
                topics = consumer.list_topics(timeout=15).topics
                last_list = time.time()
            out = []
            for name, label in PIPELINE:
                if name not in topics:
                    continue
                total = 0
                for p in topics[name].partitions:
                    lo, hi = consumer.get_watermark_offsets(TopicPartition(name, p), timeout=5, cached=False)
                    total += hi
                t = time.time()
                h = history.setdefault(name, deque())
                h.append((t, total))
                while len(h) > 2 and t - h[1][0] >= 60:
                    h.popleft()
                t0, n0 = h[0]
                rate = max(0.0, (total - n0) / (t - t0)) if t - t0 >= 5 else None
                out.append({"topic": name, "label": label, "total": total, "rate": rate})
            with state.lock:
                state.rates = {"topics": out, "t": int(time.time() * 1000)}
        except Exception as exc:
            print(f"rates: {exc!r}", file=sys.stderr)
        time.sleep(5)


def prune_forever() -> None:
    while True:
        cutoff = (time.time() - VESSEL_TTL_S) * 1000
        with state.lock:
            for mmsi in [m for m, v in state.vessels.items() if v.get("t", 0) < cutoff]:
                state.vessels.pop(mmsi, None)
            for mmsi in [m for m, d in state.dark.items() if d["last_seen"] < cutoff - 6 * 3600 * 1000]:
                state.dark.pop(mmsi, None)
        time.sleep(60)


# ---------------------------------------------------------------------------
# Snapshots for the browser
# ---------------------------------------------------------------------------
def is_service(mmsi: int) -> bool:
    return vessel_info.is_service((state.static.get(mmsi) or {}).get("ship_type"))


def vessel_row(v: dict, scores: dict) -> list:
    hdg = v["hdg"] if v.get("hdg") is not None else v.get("cog")
    return [v["mmsi"], round(v["lat"], 5), round(v["lon"], 5), round(v["sog"], 1),
            None if hdg is None else round(hdg), v.get("nav"), v.get("name"), v["t"],
            scores.get(v["mmsi"], 0),
            # over a cable: the latest fix was also a corridor fix
            1 if v.get("corridor_t", 0) >= v["t"] - 1000 else 0,
            1 if is_service(v["mmsi"]) else 0]


def public(d: dict) -> dict:
    out = {k: v for k, v in d.items() if k != "seq"}
    if "mmsi" in d:
        out["service"] = is_service(d["mmsi"])
    return out


def delta(since: int) -> dict:
    with state.lock:
        # A score covers the last hour only, so older risk states no longer count.
        cutoff = (time.time() - RISK_TTL_S) * 1000
        scores: dict[int, int] = {}
        for r in state.risk.values():
            if r["state"] != "NORMAL" and (r["t"] or 0) > cutoff:
                scores[r["mmsi"]] = max(scores.get(r["mmsi"], 0), r["score"] or 0)
        return {
            "seq": state.seq,
            "vessels": [vessel_row(v, scores) for v in state.vessels.values() if v.get("seq", 0) > since and "lat" in v],
            "risk": [public(r) for r in state.risk.values() if r["seq"] > since],
            "dark": [public(d) for d in state.dark.values() if d["seq"] > since],
            "dark_ids": list(state.dark),
            "incidents": [public(i) for i in state.incidents if i["seq"] > since],
            "rates": state.rates,
            "now": int(time.time() * 1000),
        }


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
app = FastAPI(title="Cable Watch")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.middleware("http")
async def static_cache_control(request, call_next):
    # CloudFront caches /static/* for a day unless told otherwise; five minutes
    # keeps the edge useful without serving an old app.js long after a deploy.
    response = await call_next(request)
    if request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=300"
    return response


@app.on_event("startup")
def start_threads() -> None:
    for target in (consume_forever, sample_rates_forever, prune_forever):
        threading.Thread(target=target, daemon=True).start()


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/api/config")
def api_config():
    return {
        "ntfy_url": os.environ.get("NTFY_URL", ""),
        "repo_url": os.environ.get("REPO_URL", ""),
        "tableflow": os.environ.get("TABLEFLOW_ENABLED", "true") == "true",
        "llm": os.environ.get("LLM_ENABLED", "false") == "true",
    }


@app.get("/api/cables")
def api_cables():
    return FileResponse(config.DATA_DIR / "cables.geojson", media_type="application/geo+json",
                        headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api/qr.svg")
def api_qr():
    url = os.environ.get("NTFY_URL", "")
    if not url:
        raise HTTPException(404)
    img = qrcode.make(url, image_factory=qrcode.image.svg.SvgPathImage, box_size=8, border=1)
    buf = io.BytesIO()
    img.save(buf)
    return Response(buf.getvalue(), media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api/vessel/{mmsi}")
def api_vessel(mmsi: int):
    with state.lock:
        v = state.vessels.get(mmsi)
        dark = state.dark.get(mmsi)
        static = state.static.get(mmsi)
        if v is None and dark is None:
            raise HTTPException(404)
        return {
            "mmsi": mmsi,
            "name": (static or {}).get("name") or (v or {}).get("name") or (dark or {}).get("name"),
            "about": vessel_info.describe(mmsi, static),
            "flag": vessel_info.flag(mmsi),
            "type": vessel_info.ship_type((static or {}).get("ship_type")),
            "service": vessel_info.is_service((static or {}).get("ship_type")),
            "static": {k: static.get(k) for k in ("imo", "call_sign", "destination", "length_m", "beam_m", "draught_m")}
                      if static else None,
            "track": list(v["track"]) if v else [],
            "nav": (v or {}).get("nav"),
            "nav_changes": list(v["nav_changes"]) if v else [],
            "signals": list(state.signals.get(mmsi, [])),
            "risk": [public(r) for r in state.risk.values() if r["mmsi"] == mmsi],
            "dark": public(dark) if dark else None,
            "incidents": [public(i) for i in state.incidents if i["mmsi"] == mmsi][-10:],
        }


@app.get("/api/stream")
async def api_stream():
    async def events():
        since = 0
        last_ping = time.time()
        while True:
            payload = delta(since)
            since = payload["seq"]
            yield f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
            if time.time() - last_ping > 15:
                yield ": keepalive\n\n"
                last_ping = time.time()
            await asyncio.sleep(1.0)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
