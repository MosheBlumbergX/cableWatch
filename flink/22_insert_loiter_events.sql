-- Loitering: 20-minute tumbling windows per vessel and cable over the slow fixes
-- only. A window counts if the vessel spent most of it under 2 knots over the
-- corridor. Moored vessels (nav status 5) are alongside a quay, not loitering.
-- (A session window would only close once the vessel left, so a ship that sits
-- on a cable for ten hours would raise nothing until it moved.)
INSERT INTO `loiter_events`
SELECT
  `mmsi`,
  `cable_id`,
  LAST_VALUE(`cable_name`)                                     AS `cable_name`,
  LAST_VALUE(`ship_name`)                                      AS `ship_name`,
  `window_time`                                                AS `event_time`,
  `window_start`,
  COUNT(*)                                                     AS `fixes`,
  MAX(`sog`)                                                   AS `max_sog`,
  LAST_VALUE(`lat`)                                            AS `lat`,
  LAST_VALUE(`lon`)                                            AS `lon`,
  MIN(`distance_km`)                                           AS `distance_km`,
  MAX(CAST(`replay` AS INT)) = 1                               AS `replay`
FROM TABLE(
  TUMBLE(TABLE `corridor_fixes`, DESCRIPTOR(`time_utc`), INTERVAL '20' MINUTES))
WHERE `sog` < 2.0 AND `nav_status` <> 5
GROUP BY `mmsi`, `cable_id`, `window_start`, `window_end`, `window_time`
HAVING COUNT(*) >= 3
   AND TIMESTAMPDIFF(MINUTE, MIN(`time_utc`), MAX(`time_utc`)) >= 12;
