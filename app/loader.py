"""Loads the cable geography (data/cable_cells.json.gz, built by tools/build_cables.py) into cable_cells.

One record per H3 cell, listing every cable within 10 km of it. The records are
timestamped 2020-01-01 so that, in the temporal join, this version of the
geography is valid for every vessel fix. The loader runs on every boot of the
dashboard host but only writes when the topic is empty (or with --force).
"""
import gzip
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition

import config
import serde

VALID_FROM_MS = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def already_loaded() -> bool:
    consumer = Consumer({**config.kafka_conf(), "group.id": "cablewatch-loader"})
    try:
        partitions = consumer.list_topics(config.TOPIC_CELLS, timeout=15).topics[config.TOPIC_CELLS].partitions
        return any(consumer.get_watermark_offsets(TopicPartition(config.TOPIC_CELLS, p), timeout=10)[1] > 0
                   for p in partitions)
    finally:
        consumer.close()


def main() -> None:
    if already_loaded() and "--force" not in sys.argv:
        print(f"{config.TOPIC_CELLS} already has data; nothing to do (use --force to reload)")
        return
    with gzip.open(config.DATA_DIR / "cable_cells.json.gz", "rt") as f:
        rows = json.load(f)
    cells: dict[str, list] = defaultdict(list)
    for r in rows:
        cells[r["h3_r7"]].append({
            "cable_id": r["cable_id"],
            "cable_name": r["cable_name"],
            "kind": r["kind"],
            "distance_km": r["distance_km"],
        })

    out = serde.TopicSerializer(serde.registry(), config.TOPIC_CELLS)
    producer = Producer({**config.kafka_conf(), "linger.ms": 100, "acks": "all", "compression.type": "lz4"})
    failed = 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err:
            failed += 1

    for i, (cell, cables) in enumerate(cells.items(), 1):
        record = {"h3_r7": cell, "cables": sorted(cables, key=lambda c: c["distance_km"])}
        producer.produce(config.TOPIC_CELLS, key=out.key(record), value=out.value(record),
                         timestamp=VALID_FROM_MS, on_delivery=on_delivery)
        if i % 5000 == 0:
            producer.flush()
            print(f"{i} cells")
    producer.flush(60)
    if failed:
        sys.exit(f"{failed} cells failed to deliver")
    print(f"loaded {len(cells)} cells ({len(rows)} cell/cable pairs) into {config.TOPIC_CELLS}")


if __name__ == "__main__":
    main()
