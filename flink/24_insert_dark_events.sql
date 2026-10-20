-- Going dark: a session window per vessel closes after 30 minutes without a fix.
-- The watermark is driven by every other ship in the Baltic, so a silent vessel
-- still gets a clock. An interval join fetches the session's last fix, and if
-- that was within 10 km of a cable, the vessel went dark near it.
-- Fast ships are skipped: they are usually just sailing out of receiver range.
-- Like the geofence, the cable lookup never holds back the clock (see cable_cells).
INSERT INTO `dark_events`
SELECT
  s.`mmsi`,
  x.`cable_id`,
  x.`cable_name`,
  f.`ship_name`,
  s.`window_time`  AS `event_time`,
  s.`last_seen`,
  s.`fixes`,
  f.`sog`          AS `last_sog`,
  f.`lat`,
  f.`lon`,
  x.`distance_km`
FROM (
  SELECT `mmsi`, `window_start`, `window_end`, `window_time`,
         COUNT(*)        AS `fixes`,
         MAX(`time_utc`) AS `last_seen`
  FROM TABLE(
    SESSION(TABLE `vessel_fixes` PARTITION BY `mmsi`, DESCRIPTOR(`time_utc`), INTERVAL '30' MINUTES))
  GROUP BY `mmsi`, `window_start`, `window_end`, `window_time`
) AS s
JOIN `vessel_fixes` AS f
  ON f.`mmsi` = s.`mmsi`
 AND f.`time_utc` = s.`last_seen`
 AND f.`time_utc` BETWEEN s.`window_time` - INTERVAL '31' MINUTE AND s.`window_time`
JOIN `cable_cells` FOR SYSTEM_TIME AS OF s.`window_time` AS c
  ON f.`h3_r7` = c.`h3_r7`
CROSS JOIN UNNEST(c.`cables`) AS x (`cable_id`, `cable_name`, `kind`, `distance_km`)
WHERE x.`distance_km` <= 10.0
  AND s.`fixes` >= 5
  AND f.`sog` < 8.0;
