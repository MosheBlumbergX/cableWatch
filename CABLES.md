# The cables and pipelines Cable Watch protects

Cable Watch scores vessels against **47 subsea assets** in the Baltic Sea, the
Danish straits and the Kattegat: 24 telecom cables, 19 power cables and 4 gas
pipelines. This page covers where the data comes from, how it becomes the
`cable_cells` table that Flink joins against, and what each asset is.

## Where the routes come from

All routes come from [OpenStreetMap](https://www.openstreetmap.org), queried
through the Overpass API by [`tools/build_cables.py`](tools/build_cables.py)
(`make cables`). The query takes every *named* way in the bounding box
`53.5°N 9.5°E` to `66.0°N 30.5°E` tagged as one of:

- `seamark:type=cable_submarine` or `seamark:type=pipeline_submarine` (nautical chart features)
- `power=cable`, `communication=line` or `man_made=pipeline`, with `location=underwater` or `submarine`

The build then:

1. **Groups segments by name.** OSM usually splits a cable into many ways. A few
   aliases fold together different names for the same asset, for example
   `Северный Поток` and `Nord Stream` into Nord Stream 1, and the BCS North phases
   into BCS North.
2. **Drops anything shorter than 15 km in total.** About 6,000 short, unnamed or
   inshore cables (ferry crossings, lake cables, Stockholm-archipelago power
   lines) would raise constant false alarms in harbours.
3. **Classifies each asset** as `telecom`, `power` or `pipeline` from its tags.
4. **Buffers each route by 10 km** in a metric projection (LAEA Europe,
   EPSG:3035) and fills the buffer with **H3 resolution-7 cells** (about 1.2 km
   edge, 5 km² each). Each cell records its centre's distance to the route.
5. **Drops cells within 3 km of land** (Natural Earth 10 m land and minor
   islands). Ships alongside in the port where a cable lands are not loitering
   over it.

The output is committed to the repo, so a deploy never depends on Overpass
being up:

| File | Contents | Used by |
| --- | --- | --- |
| `app/data/cables.geojson` | Simplified route lines (150 m tolerance) with name, kind, operator, length | Dashboard map |
| `app/data/cable_cells.json.gz` | 48,968 (cell, cable, distance) rows | `loader.py` → `cable_cells` topic |

## How Flink uses the cells

`loader.py` groups the rows by cell and writes **27,131 records** to the
compacted `cable_cells` topic, keyed by H3 cell. Each record lists every asset
within 10 km of that cell, nearest first:

```json
{"h3_r7": "871126536ffffff",
 "cables": [{"cable_id": "bcs-north", "cable_name": "BCS North", "kind": "telecom", "distance_km": 0.23},
            {"cable_id": "hvdc-estlink-2", "cable_name": "HVDC Estlink 2", "kind": "power", "distance_km": 1.38},
            {"cable_id": "eastern-light-sweden-finland-i", "cable_name": "Eastern Light Sweden-Finland I", "kind": "telecom", "distance_km": 4.58}]}
```

The producer tags every AIS fix with its own H3 cell, so each Flink lookup is an
equi-join on the cell, with no geometry in SQL:

- **Corridor (2 km).** `21_insert_corridor_fixes.sql` keeps fixes in a cell
  within 2 km of a route. That is 7,711 cells, about 39,000 km². Loitering and
  anchor drag are only detected here.
- **Dark radius (10 km).** `24_insert_dark_events.sql` checks a silent vessel's
  last fix against the full set of 27,131 cells.

A cell near several assets yields one row per asset, so one ship can be scored
separately against each cable it threatens. Two cables laid side by side, as in
the Gulf of Finland, raise two incidents.

## The assets

Length is the OSM line length inside the bounding box. Assets mapped as two
parallel lines (Nord Stream, some HVDC links) come out longer than the
engineering route. "Corridor" counts cells within 2 km of the route; "Watched"
counts all cells within 10 km, after the land mask.

### Telecom cables (24)

| Name | Operator (OSM) | Length km | Corridor cells | Watched cells |
| --- | --- | ---: | ---: | ---: |
| Aarhus-Kalundborg | Telia Denmark | 82 | 65 | 292 |
| Aurora | GlobalConnect | 433 | 370 | 1,860 |
| Ballen-Røsnæs | Arelion | 16 | 11 | 87 |
| Baltic Sea Submarine Cable | CITIC Telecom | 529 | 502 | 2,484 |
| Baltica | Arelion, Orange Polska, Slovak Telekom, TDC Group, Telenor, Ukrtelecom | 171 | 148 | 727 |
| Baltika (Kingisepp - Kaliningrad) | Rostelecom | 1,058 | 1,016 | 5,050 |
| BCS East | Arelion | 97 | 81 | 416 |
| BCS East-West Interlink | Arelion | 215 | 194 | 980 |
| BCS North | Arelion | 630 | 428 | 1,958 |
| C-Lion 1 | Cinia | 1,211 | 1,153 | 5,647 |
| Denmark-Northway 5 | TDC | 58 | 56 | 329 |
| Eastern Light Sweden-Finland I | Eastern Light | 444 | 378 | 1,799 |
| Fehmarn Belt | Arelion | 19 | 12 | 70 |
| Finland Estonia Connection (FEC) | Elisa | 164 | 117 | 433 |
| Finland-Estonia 2 (EESF-2) | Arelion | 72 | 69 | 332 |
| Finland-Estonia 3 (EESF-3) | Arelion | 76 | 69 | 336 |
| Great Belt 1 | Arelion | 18 | 12 | 79 |
| Great Belt 2 | Telia Denmark | 35 | 27 | 113 |
| Hou-Sælvig | Arelion | 21 | 15 | 70 |
| Latvia-Sweden 1 (LV-SE 1) | Tele2 (SE), Tet (LV) | 282 | 281 | 1,339 |
| Marika | Arelion | 102 | 61 | 345 |
| Scandinavian Ring South | Arelion | 16 | 10 | 54 |
| Skagen Fiber West | Skagenfiber | 168 | 163 | 807 |
| Sweden-Latvia | LVRTC | 163 | 149 | 749 |

### Power cables (19)

| Name | Operator (OSM) | Length km | Corridor cells | Watched cells |
| --- | --- | ---: | ---: | ---: |
| Baltic 1 (offshore wind export) | 50Hertz Transmission | 121 | 37 | 207 |
| Baltic 2 (offshore wind export) | 50Hertz Transmission | 115 | 59 | 330 |
| BEI-Interkonnektor Bornholm-Deutschland | 50Hertz Transmission | 164 | 134 | 609 |
| Bornholm Cable | – | 42 | 32 | 168 |
| Estlink 1 | EE | 79 | 55 | 270 |
| Fenno-Skan | Fingrid; Svenska Kraftnät | 399 | 286 | 1,075 |
| HVDC Baltic Cable | – | 243 | 199 | 963 |
| HVDC Estlink 2 | – | 148 | 126 | 594 |
| HVDC Gotland 2 and 3 | – | 89 | 82 | 409 |
| HVDC Konti-Skan | – | 74 | 57 | 337 |
| HVDC NordBalt | – | 395 | 362 | 1,752 |
| HVDC Storebælt I (Great Belt power link) | Energinet | 31 | 19 | 97 |
| HVDC SwePol | – | 239 | 210 | 1,033 |
| Kontek | – | 50 | 34 | 194 |
| Kriegers Flak CGS | 50Hertz Transmission | 48 | 26 | 178 |
| Ostwind 1 Ltg 261, 281 und 282 (offshore wind export) | 50Hertz Transmission | 83 | 69 | 358 |
| Ostwind 2 (offshore wind export) | 50Hertz Transmission | 76 | 68 | 326 |
| Ostwind 2-1 (offshore wind export) | 50Hertz Transmission | 18 | 18 | 155 |
| Ostwind 3 (offshore wind export) | – | 102 | 87 | 410 |

### Gas pipelines (4)

| Name | Operator (OSM) | Length km | Corridor cells | Watched cells |
| --- | --- | ---: | ---: | ---: |
| Baltic Connector (Balticconnector) | – | 78 | 68 | 334 |
| Baltic Pipe | Energinet; Gaz-System | 277 | 250 | 1,202 |
| Nord Stream 1 | Nord Stream AG | 1,371 | 1,170 | 5,779 |
| Nord Stream 2 | Nord Stream AG | 1,228 | 1,175 | 5,832 |

Pipelines appear on the map as dashed magenta lines and cables as wavy ones,
following the conventions of nautical charts.

## Incidents that motivated the demo

Several of these assets were damaged in 2022 to 2025, and that is the reason
the project exists. In each anchor case the suspect vessel's behaviour (slow
passage over the corridor, an anchor dragged along the seabed, gaps in its AIS
track) was visible afterwards in public AIS data:

| When | Asset(s) | What happened |
| --- | --- | --- |
| September 2022 | Nord Stream 1 and 2 | Destroyed by underwater explosions, not an anchor. Included because the pipelines remain on the seabed and on charts. |
| October 2023 | Balticconnector, plus telecom cables between Estonia and Finland and Sweden | Damaged by the dragged anchor of the container ship *Newnew Polar Bear*. |
| November 2024 | BCS East-West Interlink, C-Lion 1 | Cut within a day of each other; the bulk carrier *Yi Peng 3* was the suspect vessel. |
| December 2024 | HVDC Estlink 2, plus four Finland–Estonia telecom cables | Damaged by the dragged anchor of the tanker *Eagle S*. |

These summaries follow public reporting at the time. They describe what was
reported, not the outcome of any investigation.

## Limitations

- **OSM is a crowd-sourced map, not a cable registry.** Routes are approximate,
  and some are incomplete: BCS East-West Interlink is mapped for 215 km of its
  route. A cable missing from OSM, or mapped without a name, is not watched.
  [TeleGeography's submarine cable map](https://www.submarinecablemap.com) is
  more complete for telecom cables, but its licence does not allow
  redistribution.
- **The 3 km land mask is coarse.** It also hides real exposure in narrow
  straits and close to shore, where several of the Danish crossings run.
- **Distances are cell-centre distances.** A fix can be up to about 1.4 km from
  its cell's centre, so "within 2 km" really means 2 km, give or take a cell.
- **Cables can share a corridor.** In the Gulf of Finland several run within a
  kilometre of each other, so one ship raises an incident per cable.

## Changing the set

Edit the constants at the top of `tools/build_cables.py` and run `make cables`:

| Constant | Default | Effect |
| --- | --- | --- |
| `BBOX` | `(53.5, 9.5, 66.0, 30.5)` | Area to query (south, west, north, east) |
| `MIN_LENGTH_KM` | 15 | Drop assets shorter than this |
| `OUTER_KM` | 10 | Buffer filled with cells (the dark-vessel radius) |
| `MIN_SHORE_KM` | 3 | Drop cells closer than this to land |
| `ALIASES` | | Map alternative OSM names onto one asset |

Then commit `app/data`. The loader only writes to `cable_cells` when the topic
is empty, so to load a new geography into a running deployment:

```sh
make env && cd app && python loader.py --force
```

Cells that disappear from the new geography keep their old record in the
compacted topic until you delete them (write a tombstone for the key).

## Licence

Cable and pipeline routes © OpenStreetMap contributors, available under the
[Open Database Licence (ODbL)](https://www.openstreetmap.org/copyright). The
derived files in `app/data` are a derivative database under the same licence:
keep the attribution, which the dashboard shows. The land mask is from
[Natural Earth](https://www.naturalearthdata.com) (public domain).
