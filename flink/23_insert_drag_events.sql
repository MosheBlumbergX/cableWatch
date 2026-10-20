-- Anchor drag (the Eagle S signature): eight consecutive corridor fixes moving at
-- 1.5 to 7 knots while either reporting "at anchor" (nav status 1) or moving more
-- than 45 degrees off the bow. Fires again for every further eight fixes.
INSERT INTO `drag_events`
SELECT
  `mmsi`,
  `cable_id`,
  `cable_name`,
  `ship_name`,
  `event_time`,
  `start_time`,
  `fixes`,
  `avg_sog`,
  `lat`,
  `lon`,
  `distance_km`
FROM `corridor_fixes`
MATCH_RECOGNIZE (
  PARTITION BY `mmsi`, `cable_id`
  ORDER BY `time_utc`
  MEASURES
    MATCH_ROWTIME()          AS `event_time`,
    FIRST(D.`time_utc`)      AS `start_time`,
    LAST(D.`cable_name`)     AS `cable_name`,
    LAST(D.`ship_name`)      AS `ship_name`,
    COUNT(D.`mmsi`)          AS `fixes`,
    AVG(D.`sog`)             AS `avg_sog`,
    LAST(D.`lat`)            AS `lat`,
    LAST(D.`lon`)            AS `lon`,
    MIN(D.`distance_km`)     AS `distance_km`
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (D{8}) WITHIN INTERVAL '30' MINUTE
  DEFINE
    D AS D.`sog` BETWEEN 1.5 AND 7.0
     AND (D.`nav_status` = 1
          OR (D.`heading` IS NOT NULL AND D.`cog` IS NOT NULL
              AND ABS(MOD(CAST(D.`heading` - D.`cog` AS INT) + 540, 360) - 180) > 45))
);
