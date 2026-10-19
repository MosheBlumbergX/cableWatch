-- Score per vessel and cable, written by one statement so that risk_updates has
-- a single writer. (Two jobs writing one topic commit their transactions at
-- different checkpoints; their rows interleave out of event-time order and the
-- incident statement downstream drops the "late" ones.)
--
-- 1. Running score: the points of every signal in the last hour.
--    NORMAL under 30, WATCH 30-59, ALERT 60+. Heartbeat rows (mmsi 0) pass
--    through with score 0, which keeps risk_updates' watermark moving as well.
-- 2. Decay: once a vessel has raised no signal over a cable for an hour, its
--    score drops to zero. A session window with a one-hour gap closes at exactly
--    that moment and emits a CLEAR.
INSERT INTO `risk_updates`
SELECT
  `mmsi`,
  `cable_id`,
  `cable_name`,
  `ship_name`,
  `event_time`,
  `signal`,
  `points`,
  CAST(`score` AS INT),
  CASE WHEN `score` >= 60 THEN 'ALERT' WHEN `score` >= 30 THEN 'WATCH' ELSE 'NORMAL' END,
  CAST(`loiters` AS INT),
  CAST(`drags` AS INT),
  CAST(`darks` AS INT),
  `lat`,
  `lon`,
  `distance_km`,
  `sog`,
  `replay`
FROM (
  SELECT *,
    SUM(`points`) OVER w                                        AS `score`,
    SUM(CASE WHEN `signal` = 'LOITER' THEN 1 ELSE 0 END) OVER w AS `loiters`,
    SUM(CASE WHEN `signal` = 'DRAG' THEN 1 ELSE 0 END) OVER w   AS `drags`,
    SUM(CASE WHEN `signal` = 'DARK' THEN 1 ELSE 0 END) OVER w   AS `darks`
  FROM `risk_signals`
  WINDOW w AS (
    PARTITION BY `mmsi`, `cable_id`
    ORDER BY `event_time`
    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
)
UNION ALL
SELECT
  `mmsi`,
  `cable_id`,
  MAX(`cable_name`),
  MAX(`ship_name`),
  `window_time`,
  CAST('CLEAR' AS STRING),
  0,
  0,
  CAST('NORMAL' AS STRING),
  0,
  0,
  0,
  CAST(NULL AS DOUBLE),
  CAST(NULL AS DOUBLE),
  CAST(NULL AS DOUBLE),
  CAST(NULL AS DOUBLE),
  MAX(CAST(`replay` AS INT)) = 1
FROM TABLE(
  SESSION(TABLE `risk_signals` PARTITION BY (`mmsi`, `cable_id`), DESCRIPTOR(`event_time`), INTERVAL '1' HOUR))
WHERE `signal` <> 'HEARTBEAT'
GROUP BY `mmsi`, `cable_id`, `window_start`, `window_end`, `window_time`;
