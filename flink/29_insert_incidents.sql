-- Turn score changes into incidents: entering WATCH, entering ALERT (TRIGGER) and
-- dropping back to NORMAL (RESOLVE). Enriched with the vessel's latest static data.
-- Relies on risk_updates' 2-minute watermark allowance (see its CREATE TABLE).
INSERT INTO `incidents`
SELECT
  `mmsi`,
  `cable_id`,
  `event_time`,
  `action`,
  `state`,
  `prev_state`,
  `score`,
  `signal`,
  `cable_name`,
  `vessel_name`,
  `vessel_type`,
  `imo`,
  `destination`,
  `lat`,
  `lon`,
  `sog`,
  `distance_km`,
  CONCAT(
    `vessel_name`, ' ',
    CASE
      WHEN `action` = 'RESOLVE'  THEN 'clear of '
      WHEN `signal` = 'DRAG'     THEN 'dragging anchor over '
      WHEN `signal` = 'LOITER'   THEN 'loitering over '
      ELSE 'went dark near '
    END,
    `cable_name`
  ) AS `summary`,
  CASE WHEN `action` = 'RESOLVE' THEN
    CONCAT(`vessel_name`, ' has raised no signal over ', `cable_name`,
           ' for an hour; risk score back to 0. Incident closed automatically.')
  ELSE CONCAT(
    `vessel_name`, ' (', `vessel_type`,
    CASE WHEN `imo` > 0 THEN CONCAT(', IMO ', CAST(`imo` AS STRING)) ELSE '' END,
    ') is ',
    CASE
      WHEN `signal` = 'DRAG'   THEN CONCAT('moving at ', CAST(ROUND(`sog`, 1) AS STRING), ' kn with its anchor down or sideways ')
      WHEN `signal` = 'LOITER' THEN 'stationary '
      ELSE 'silent, last seen '
    END,
    CAST(ROUND(`distance_km`, 1) AS STRING), ' km from ', `cable_name`,
    ' at ', CAST(ROUND(`lat`, 3) AS STRING), 'N ', CAST(ROUND(`lon`, 3) AS STRING), 'E. ',
    'Risk score ', CAST(`score` AS STRING), ' in the last hour from ',
    CAST(`loiter_count` AS STRING), ' loiter, ', CAST(`drag_count` AS STRING), ' drag and ',
    CAST(`dark_count` AS STRING), ' dark signal(s)',
    CASE WHEN `watch_reason` IS NOT NULL
         THEN CONCAT(', plus ', CAST(`watch_points` AS STRING), ' watch-list points (', `watch_reason`, ')') ELSE '' END,
    CASE WHEN `destination` IS NOT NULL AND `destination` <> '' THEN CONCAT('; declared destination ', `destination`) ELSE '' END,
    '.')
  END AS `brief`,
  CONCAT(
    'Vessel: ', `vessel_name`, ' (', `vessel_type`, ', MMSI ', CAST(`mmsi` AS STRING),
    CASE WHEN `imo` > 0 THEN CONCAT(', IMO ', CAST(`imo` AS STRING)) ELSE '' END, '). ',
    'Infrastructure: ', `cable_name`, '. Distance: ', CAST(ROUND(`distance_km`, 1) AS STRING), ' km. ',
    'Position: ', CAST(ROUND(`lat`, 3) AS STRING), 'N ', CAST(ROUND(`lon`, 3) AS STRING), 'E. ',
    'Latest signal: ', `signal`, '. Speed: ', COALESCE(CAST(ROUND(`sog`, 1) AS STRING), 'unknown'), ' kn. ',
    'Signals in the last hour: ', CAST(`loiter_count` AS STRING), ' x loitering (under 2 kn for 20 min), ',
    CAST(`drag_count` AS STRING), ' x anchor drag, ', CAST(`dark_count` AS STRING), ' x AIS went silent. ',
    'Risk score: ', CAST(`score` AS STRING), ' (', `state`, '). ',
    'Watch list: ', COALESCE(`watch_reason`, 'none'), '. ',
    'Destination: ', COALESCE(`destination`, 'unknown'), '.'
  ) AS `prompt`,
  `watch_reason`
FROM (
  SELECT
    r.*,
    COALESCE(NULLIF(TRIM(s.`name`), ''), r.`ship_name`, CONCAT('MMSI ', CAST(r.`mmsi` AS STRING))) AS `vessel_name`,
    CASE
      WHEN s.`ship_type` IS NULL                THEN 'type unknown'
      WHEN s.`ship_type` = 30                   THEN 'fishing vessel'
      WHEN s.`ship_type` IN (31, 32, 52)        THEN 'tug'
      WHEN s.`ship_type` = 33                   THEN 'dredger'
      WHEN s.`ship_type` = 34                   THEN 'dive vessel'
      WHEN s.`ship_type` = 35                   THEN 'military vessel'
      WHEN s.`ship_type` = 36                   THEN 'sailing vessel'
      WHEN s.`ship_type` = 37                   THEN 'pleasure craft'
      WHEN s.`ship_type` BETWEEN 40 AND 49      THEN 'high-speed craft'
      WHEN s.`ship_type` = 50                   THEN 'pilot vessel'
      WHEN s.`ship_type` = 51                   THEN 'search and rescue vessel'
      WHEN s.`ship_type` = 55                   THEN 'law enforcement vessel'
      WHEN s.`ship_type` BETWEEN 60 AND 69      THEN 'passenger ship'
      WHEN s.`ship_type` BETWEEN 70 AND 79      THEN 'cargo ship'
      WHEN s.`ship_type` BETWEEN 80 AND 89      THEN 'tanker'
      ELSE 'other type'
    END AS `vessel_type`,
    s.`imo`,
    NULLIF(TRIM(s.`destination`), '') AS `destination`,
    CASE
      WHEN r.`state` = 'ALERT'  THEN 'TRIGGER'
      WHEN r.`state` = 'NORMAL' THEN 'RESOLVE'
      WHEN r.`state` = 'WATCH' AND COALESCE(r.`prev_state`, 'NORMAL') = 'NORMAL' THEN 'WATCH'
    END AS `action`
  FROM (
    -- Previous state via a two-row ROWS frame rather than LAG: LAG uses a RANGE
    -- frame, where two updates with the same timestamp see each other.
    SELECT *,
      CASE WHEN COUNT(*) OVER w = 1 THEN CAST(NULL AS STRING) ELSE FIRST_VALUE(`state`) OVER w END AS `prev_state`
    FROM `risk_updates`
    WINDOW w AS (PARTITION BY `mmsi`, `cable_id` ORDER BY `event_time` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
  ) AS r
  LEFT JOIN `vessel_static` FOR SYSTEM_TIME AS OF r.`event_time` AS s
    ON r.`mmsi` = s.`mmsi`
  WHERE r.`signal` <> 'HEARTBEAT'
    AND r.`state` <> COALESCE(r.`prev_state`, 'NORMAL')
)
WHERE `action` IS NOT NULL;
