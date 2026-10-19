"""Replays an Eagle S-style incident over Estlink 2, so the demo always has an alert.

A synthetic track, loosely after the public reporting on the Eagle S (25 December
2024): a laden tanker heading west down the Gulf of Finland slows over the
Estlink 2 corridor and drags its anchor across it, then stops and its AIS falls
silent. Positions go through the same producer path (same data contract, same
topic) with replay = true, and the same Flink statements raise the incident.

Event time is "now": the track plays in real time, one fix every 10 seconds, so
it never disturbs the watermark for the live vessels sharing its partition.
Expect WATCH about 8 minutes in, ALERT a minute or two later, a DARK signal 30
minutes after the last fix, and the incident resolving an hour after that.
"""
import argparse
import gzip
import json
import math
import signal
import time
from datetime import datetime, timezone

import h3

import config
from producer import AisWriter, valid_position

MMSI = 518998494  # Eagle S
STATIC = {
    "mmsi": MMSI, "name": "EAGLE S", "call_sign": "E5U4248", "imo": 9329760, "ship_type": 80,
    "destination": "PORT SAID", "length_m": 228, "beam_m": 32, "draught_m": 13.2,
}
CABLE_ID = "hvdc-estlink-2"
COURSE = 262.0  # westbound down the Gulf of Finland
STEP_S = 10

running = True


def stop(*_):
    global running
    running = False


def km_to_deg(lat: float, dx_km: float, dy_km: float) -> tuple[float, float]:
    return dy_km / 110.57, dx_km / (111.32 * math.cos(math.radians(lat)))


def crossing_point() -> tuple[float, float]:
    """A point on Estlink 2 that lies in an offshore corridor cell."""
    corridor = set()
    with gzip.open(config.DATA_DIR / "cable_cells.json.gz", "rt") as f:
        for c in json.load(f):
            if c["cable_id"] == CABLE_ID and c["distance_km"] <= 0.6:
                corridor.add(c["h3_r7"])
    cables = json.loads((config.DATA_DIR / "cables.geojson").read_text())
    feature = next(f for f in cables["features"] if f["properties"]["cable_id"] == CABLE_ID)
    line = max(feature["geometry"]["coordinates"], key=len)
    # Walk outwards from the middle of the longest segment until the point is in an offshore cell.
    mid = len(line) // 2
    for offset in range(len(line)):
        for i in (mid + offset, mid - offset):
            if 0 < i < len(line) - 1:
                lon, lat = line[i]
                if h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION) in corridor:
                    return lat, lon
    raise SystemExit("no offshore corridor cell found on Estlink 2")


def track() -> list[dict]:
    """(lat, lon, sog, cog, heading, nav_status) every STEP_S seconds."""
    lat0, lon0 = crossing_point()
    course = COURSE
    phases = (
        # (minutes, sog, nav_status)
        (2, 9.5, 0),    # approach at passage speed
        (1, 5.5, 0),    # slowing down
        (9, 4.2, 1),    # anchor down, still making 4 knots: "at anchor" while moving
        (3, 0.4, 1),    # stopped over the corridor
    )
    # Start so that the cable is crossed half way through the drag.
    km = lambda minutes, sog: minutes / 60 * sog * 1.852
    total_km = km(*phases[0][:2]) + km(*phases[1][:2]) + km(*phases[2][:2]) / 2
    rad = math.radians(course)
    dlat, dlon = km_to_deg(lat0, -total_km * math.sin(rad), -total_km * math.cos(rad))
    lat, lon = lat0 + dlat, lon0 + dlon
    fixes = []
    for minutes, sog, nav in phases:
        for _ in range(int(minutes * 60 / STEP_S)):
            jitter = math.sin(len(fixes) / 3.0)
            speed = max(0.0, sog + 0.3 * jitter)
            cog = (course + 4 * jitter) % 360
            fixes.append({"lat": lat, "lon": lon, "sog": round(speed, 1), "cog": round(cog, 1),
                          "heading": int(course), "nav_status": nav})
            step_km = speed * 1.852 * STEP_S / 3600
            dlat, dlon = km_to_deg(lat, step_km * math.sin(math.radians(cog)), step_km * math.cos(math.radians(cog)))
            lat, lon = lat + dlat, lon + dlon
    return fixes


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="print the track instead of producing it")
    args = ap.parse_args()
    fixes = track()
    if args.dry_run:
        for f in fixes:
            print(json.dumps(f))
        return

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    writer = AisWriter()
    writer.vessel(STATIC)
    print(f"replaying {len(fixes)} fixes ({len(fixes) * STEP_S // 60} min) for EAGLE S over Estlink 2")
    started = time.monotonic()
    for i, f in enumerate(fixes):
        if not running:
            break
        now = datetime.now(timezone.utc)
        writer.position({
            "mmsi": MMSI, "time_utc": now, "ais_second": now.second,
            "lat": f["lat"], "lon": f["lon"], "sog": f["sog"], "cog": f["cog"],
            "heading": f["heading"], "nav_status": f["nav_status"], "msg_type": "PositionReport",
            "h3_r7": h3.latlng_to_cell(f["lat"], f["lon"], config.H3_RESOLUTION) if valid_position(f["lat"], f["lon"]) else None,
            "ship_name": "EAGLE S", "replay": True,
        })
        if i % 18 == 0:
            writer.vessel(STATIC)  # static data repeats every few minutes, as on a real transponder
        writer.poll()
        time.sleep(max(0.0, started + (i + 1) * STEP_S - time.monotonic()))
    writer.flush()
    print("replay finished; the vessel has gone dark")


if __name__ == "__main__":
    main()
