-- Geofence as an equi-join: the producer tags every fix with its H3 cell, and an
-- event-time temporal join looks the cell up in the cable geography. Only fixes
-- within 2 km of a cable route are kept, once per cable. (cable_cells' far-future
-- watermark means only vessel_fixes drives this join's clock.)
INSERT INTO `corridor_fixes`
SELECT
  f.`mmsi`,
  x.`cable_id`,
  f.`time_utc`,
  f.`lat`,
  f.`lon`,
  f.`sog`,
  f.`cog`,
  f.`heading`,
  f.`nav_status`,
  f.`ship_name`,
  x.`cable_name`,
  x.`kind`,
  x.`distance_km`
FROM `vessel_fixes` AS f
JOIN `cable_cells` FOR SYSTEM_TIME AS OF f.`time_utc` AS c
  ON f.`h3_r7` = c.`h3_r7`
CROSS JOIN UNNEST(c.`cables`) AS x (`cable_id`, `cable_name`, `kind`, `distance_km`)
WHERE x.`distance_km` <= 2.0;
