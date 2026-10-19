"""Streams live AIS from aisstream.io into Kafka.

Position reports (Class A and B) go to ais_position_raw, tagged with their H3
resolution-7 cell so Flink can geofence with an equi-join. ShipStaticData goes
to vessel_static. Both are Avro, written with schemas that already exist (the
raw topic's carries the data contract; vessel_static's was created by Flink).

Reports that break a data contract rule (latitude 91, speed 102.3, ...) are
diverted to ais_position_dlq by the serializer itself and counted here.
"""
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import h3
import websockets
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationError

import config
import serde

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
STATS_EVERY_S = 30

running = True


def stop(*_):
    global running
    running = False


def parse_time(meta: dict) -> datetime:
    # "2026-10-19 14:47:55.00980646 +0000 UTC": nanoseconds, which datetime can't parse.
    date, clock = meta["time_utc"].split(" ")[:2]
    whole, _, frac = clock.partition(".")
    return datetime.fromisoformat(f"{date}T{whole}.{(frac + '000000')[:6]}+00:00")


def valid_position(lat: float, lon: float) -> bool:
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


def position_record(msg: dict, replay: bool = False) -> dict:
    kind = msg["MessageType"]
    report = msg["Message"][kind]
    meta = msg["MetaData"]
    lat, lon = float(report["Latitude"]), float(report["Longitude"])
    return {
        "mmsi": int(report["UserID"]),
        "time_utc": parse_time(meta),
        "ais_second": int(report.get("Timestamp", 60)),
        "lat": lat,
        "lon": lon,
        "sog": float(report["Sog"]),
        "cog": float(report["Cog"]),
        "heading": int(report.get("TrueHeading", 511)),
        "nav_status": int(report.get("NavigationalStatus", 15)),
        "msg_type": kind,
        "h3_r7": h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION) if valid_position(lat, lon) else None,
        "ship_name": (meta.get("ShipName") or "").strip() or None,
        "replay": replay,
    }


def static_record(msg: dict) -> dict:
    data = msg["Message"]["ShipStaticData"]
    dim = data.get("Dimension") or {}
    return {
        "mmsi": int(data["UserID"]),
        "name": (data.get("Name") or "").strip() or None,
        "call_sign": (data.get("CallSign") or "").strip() or None,
        "imo": int(data.get("ImoNumber") or 0) or None,
        "ship_type": int(data.get("Type") or 0) or None,
        "destination": (data.get("Destination") or "").strip() or None,
        "length_m": (int(dim.get("A", 0)) + int(dim.get("B", 0))) or None,
        "beam_m": (int(dim.get("C", 0)) + int(dim.get("D", 0))) or None,
        "draught_m": float(data.get("MaximumStaticDraught") or 0) or None,
    }


class AisWriter:
    """Serializes and produces positions and static data; shared with replay.py."""

    def __init__(self):
        serde.register_rules()
        sr = serde.registry()
        self.raw = serde.TopicSerializer(sr, config.TOPIC_RAW, avro_key=False)
        self.static = serde.TopicSerializer(sr, config.TOPIC_STATIC)
        self.producer = Producer({**config.kafka_conf(), "linger.ms": 50, "acks": "all", "compression.type": "lz4"})
        self.counts = {"positions": 0, "static": 0, "dlq": 0, "errors": 0}

    def _on_delivery(self, err, _msg):
        if err:
            self.counts["errors"] += 1
            print(f"delivery failed: {err}", file=sys.stderr)

    def position(self, record: dict) -> bool:
        try:
            value = self.raw.value(record)
        except SerializationError:
            # The DLQ rule action already wrote the record to ais_position_dlq.
            self.counts["dlq"] += 1
            return False
        self.producer.produce(
            config.TOPIC_RAW,
            key=str(record["mmsi"]).encode(),
            value=value,
            timestamp=int(record["time_utc"].timestamp() * 1000),
            on_delivery=self._on_delivery,
        )
        self.counts["positions"] += 1
        return True

    def vessel(self, record: dict, timestamp_ms: int | None = None) -> None:
        self.producer.produce(
            config.TOPIC_STATIC,
            key=self.static.key(record),
            value=self.static.value(record),
            timestamp=timestamp_ms or int(time.time() * 1000),
            on_delivery=self._on_delivery,
        )
        self.counts["static"] += 1

    def poll(self) -> None:
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush(10)


async def stream(writer: AisWriter, api_key: str) -> None:
    subscription = {
        "APIKey": api_key,
        "BoundingBoxes": [config.BOUNDING_BOX],
        "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport", "ShipStaticData"],
    }
    async with websockets.connect(AISSTREAM_URL, ping_interval=20, max_queue=4096) as ws:
        await ws.send(json.dumps(subscription))
        print("subscribed to aisstream.io (Baltic Sea)")
        last_stats = time.monotonic()
        while running:
            raw = await asyncio.wait_for(ws.recv(), timeout=60)
            msg = json.loads(raw)
            kind = msg.get("MessageType")
            try:
                if kind in ("PositionReport", "StandardClassBPositionReport"):
                    writer.position(position_record(msg))
                elif kind == "ShipStaticData":
                    writer.vessel(static_record(msg))
                elif "error" in msg:
                    raise SystemExit(f"aisstream error: {msg['error']}")
            except (KeyError, TypeError, ValueError) as exc:
                writer.counts["errors"] += 1
                print(f"skipped malformed {kind}: {exc}", file=sys.stderr)
            writer.poll()
            if time.monotonic() - last_stats > STATS_EVERY_S:
                last_stats = time.monotonic()
                print(" ".join(f"{k}={v}" for k, v in writer.counts.items()))


async def main() -> None:
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    api_key = os.environ["AISSTREAM_API_KEY"]
    writer = AisWriter()
    backoff = 2
    while running:
        try:
            await stream(writer, api_key)
            backoff = 2
        except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as exc:
            print(f"websocket: {exc!r}; reconnecting in {backoff}s", file=sys.stderr)
            writer.flush()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    writer.flush()
    print("stopped")


if __name__ == "__main__":
    asyncio.run(main())
