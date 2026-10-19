-- Clean and dedupe. Several base stations relay the same report; keep the first
-- copy (same vessel, same AIS second, same position). "Not available" heading
-- and course become NULL. Fixes with impossible coordinates never get here:
-- the data contract sent them to ais_position_dlq.
INSERT INTO `vessel_fixes`
SELECT
  `mmsi`,
  `time_utc`,
  `lat`,
  `lon`,
  `sog`,
  CASE WHEN `cog` >= 0 AND `cog` < 360 THEN `cog` END              AS `cog`,
  CASE WHEN `heading` BETWEEN 0 AND 359 THEN `heading` END         AS `heading`,
  `nav_status`,
  `h3_r7`,
  NULLIF(TRIM(`ship_name`), '')                                    AS `ship_name`,
  `replay`
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY `mmsi`, `ais_second`, `lat`, `lon` ORDER BY `time_utc`) AS `rn`
  FROM `ais_position_raw`
  WHERE `h3_r7` IS NOT NULL
)
WHERE `rn` = 1;
