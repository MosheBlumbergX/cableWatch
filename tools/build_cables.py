"""Builds the cable geography from OpenStreetMap: app/data/cables.geojson and cable_cells.json.

Run once on a laptop (`make cables`); the output is committed so the demo never
depends on Overpass being up. OSM data is ODbL: keep the attribution on the map.

  1. Overpass: every named submarine cable and pipeline in the Baltic box.
  2. Group segments by name into assets; drop short ones (inshore ferry and lake cables).
  3. Buffer each asset by OUTER_KM, fill with H3 resolution-7 cells, and record each
     cell's distance to the route. Flink uses distance <= 2 km for the corridor and
     <= 10 km for "went dark near a cable".
  4. Drop cells within MIN_SHORE_KM of land (Natural Earth), so ships alongside in
     the port where a cable lands are not treated as loitering over it.
"""
import argparse
import gzip
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import h3
import httpx
from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString, Point, mapping, shape
import shapely
from shapely.ops import unary_union
from shapely.prepared import prep

BBOX = (53.5, 9.5, 66.0, 30.5)  # south, west, north, east
RES = 7
OUTER_KM = 10.0
MIN_LENGTH_KM = 15.0
MIN_SHORE_KM = 3.0

OVERPASS = ["https://overpass-api.de/api/interpreter", "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.private.coffee/api/interpreter"]
NE = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson"
UA = {"User-Agent": "cablewatch-demo/0.1 (https://github.com/MosheBlumbergX/cableWatch)", "Accept": "application/json"}

QUERY = """[out:json][timeout:170];
(
 way["seamark:type"="cable_submarine"]["name"]({bbox});
 way["seamark:type"="pipeline_submarine"]["name"]({bbox});
 way["power"="cable"]["location"~"underwater|submarine"]["name"]({bbox});
 way["communication"="line"]["location"~"underwater|submarine"]["name"]({bbox});
 way["man_made"="pipeline"]["location"~"underwater|submarine"]["name"]({bbox});
);
out tags geom;"""

# Same asset, different OSM names.
ALIASES = {
    "Nord Stream": "Nord Stream 1",
    "Северный Поток": "Nord Stream 1",
    "Fenno-Skan elektrodi": "Fenno-Skan",
    "BCS North - Phase 1": "BCS North",
    "BCS North - Phase 2": "BCS North",
}

_to_m = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True).transform  # LAEA Europe, metres
_to_deg = Transformer.from_crs("EPSG:3035", "EPSG:4326", always_xy=True).transform


def to_m(geom):
    return shapely.transform(geom, lambda x, y: _to_m(x, y), interleaved=False)


def to_deg(geom):
    return shapely.transform(geom, lambda x, y: _to_deg(x, y), interleaved=False)


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def kind_of(tags: dict) -> str:
    if tags.get("seamark:type") == "pipeline_submarine" or tags.get("man_made") == "pipeline":
        return "pipeline"
    if tags.get("power") == "cable" or tags.get("seamark:cable_submarine:category") == "power":
        return "power"
    return "telecom"


def fetch(urls: str | list[str], cache: Path, method: str = "GET", **kw) -> dict:
    if cache.exists():
        return json.loads(cache.read_text())
    mirrors = [urls] if isinstance(urls, str) else urls
    for attempt in range(4 * len(mirrors)):
        url = mirrors[attempt % len(mirrors)]
        if attempt >= len(mirrors):
            time.sleep(10 * (attempt // len(mirrors)))  # public Overpass servers are often at capacity
        print(f"fetching {url}", file=sys.stderr)
        try:
            resp = httpx.request(method, url, headers=UA, timeout=300, follow_redirects=True, **kw)
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            print(f"  failed: {exc}", file=sys.stderr)
            continue
        cache.write_text(resp.text)
        return data
    raise SystemExit("all mirrors failed")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", type=Path, default=Path(".cache"))
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parents[1] / "app" / "data")
    args = ap.parse_args()
    args.cache.mkdir(exist_ok=True)
    args.out.mkdir(parents=True, exist_ok=True)

    bbox = ",".join(str(v) for v in BBOX)
    osm = fetch(OVERPASS, args.cache / "overpass.json", method="POST", data={"data": QUERY.format(bbox=bbox)})
    land = [shape(f["geometry"]) for f in fetch(f"{NE}/ne_10m_land.geojson", args.cache / "land.geojson")["features"]]
    land += [shape(f["geometry"]) for f in fetch(f"{NE}/ne_10m_minor_islands.geojson", args.cache / "islands.geojson")["features"]]
    region = shape({"type": "Polygon", "coordinates": [[(BBOX[1] - 2, BBOX[0] - 2), (BBOX[3] + 2, BBOX[0] - 2),
                                                         (BBOX[3] + 2, BBOX[2] + 2), (BBOX[1] - 2, BBOX[2] + 2),
                                                         (BBOX[1] - 2, BBOX[0] - 2)]]})
    land_m = to_m(unary_union([g.intersection(region) for g in land if g.intersects(region)]))
    land_prepared = prep(land_m.buffer(MIN_SHORE_KM * 1000))

    assets: dict[str, dict] = defaultdict(lambda: {"lines": [], "operators": set(), "kinds": defaultdict(int)})
    for el in osm["elements"]:
        tags = el["tags"]
        name = (tags.get("name") or tags.get("seamark:name")).strip()
        name = ALIASES.get(name, name)
        coords = [(p["lon"], p["lat"]) for p in el.get("geometry", [])]
        if len(coords) < 2:
            continue
        a = assets[name]
        a["lines"].append(LineString(coords))
        a["kinds"][kind_of(tags)] += len(coords)
        if tags.get("operator"):
            a["operators"].add(tags["operator"])

    features, cells = [], []
    for name, a in sorted(assets.items()):
        geom_m = to_m(unary_union(a["lines"]))
        length_km = geom_m.length / 1000
        if length_km < MIN_LENGTH_KM:
            continue
        cable_id = slug(name)
        kind = max(a["kinds"], key=a["kinds"].get)
        operator = "; ".join(sorted(a["operators"]))[:120]

        buffer_deg = to_deg(geom_m.buffer(OUTER_KM * 1000))
        kept = 0
        for cell in h3.geo_to_cells(mapping(buffer_deg), RES):
            lat, lon = h3.cell_to_latlng(cell)
            centre = Point(_to_m(lon, lat))
            if land_prepared.contains(centre):
                continue
            cells.append({
                "h3_r7": cell,
                "cable_id": cable_id,
                "cable_name": name,
                "kind": kind,
                "operator": operator,
                "distance_km": round(geom_m.distance(centre) / 1000, 2),
            })
            kept += 1
        if not kept:
            continue

        simple = to_deg(geom_m.simplify(150))
        lines = list(simple.geoms) if isinstance(simple, MultiLineString) else [simple]
        features.append({
            "type": "Feature",
            "properties": {"cable_id": cable_id, "name": name, "kind": kind, "operator": operator,
                           "length_km": round(length_km)},
            "geometry": mapping(MultiLineString([ln for ln in lines if isinstance(ln, LineString)])),
        })
        print(f"{name:45s} {kind:9s} {length_km:7.0f} km {kept:6d} cells", file=sys.stderr)

    (args.out / "cables.geojson").write_text(json.dumps({
        "type": "FeatureCollection",
        "attribution": "Cable routes © OpenStreetMap contributors, ODbL",
        "features": features,
    }, separators=(",", ":")))
    with gzip.open(args.out / "cable_cells.json.gz", "wt") as f:
        json.dump(cells, f, separators=(",", ":"))
    near = sum(1 for c in cells if c["distance_km"] <= 2.0)
    print(f"{len(features)} assets, {len(cells)} cells within {OUTER_KM:g} km ({near} within 2 km)", file=sys.stderr)


if __name__ == "__main__":
    main()
