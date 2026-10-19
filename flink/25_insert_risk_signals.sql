-- Union the detectors and attach the points each signal is worth.
--
-- Plus a heartbeat: one row every 30 seconds, from a tumbling window over the
-- always-busy vessel_fixes. Signals are rare, and the event-time operators
-- downstream (the running score, the decay session, the incident LAG) only
-- emit once a later row moves their watermark past a signal. Without the
-- heartbeat an incident would wait for the next signal anywhere in the Baltic.
INSERT INTO `risk_signals`
SELECT `mmsi`, `cable_id`, `cable_name`, `ship_name`, `event_time`,
       CAST('LOITER' AS STRING) AS `signal`, 20 AS `points`,
       `lat`, `lon`, `distance_km`, `max_sog` AS `sog`, `replay`
FROM `loiter_events`
UNION ALL
SELECT `mmsi`, `cable_id`, `cable_name`, `ship_name`, `event_time`,
       CAST('DRAG' AS STRING), 50,
       `lat`, `lon`, `distance_km`, `avg_sog`, `replay`
FROM `drag_events`
UNION ALL
SELECT `mmsi`, `cable_id`, `cable_name`, `ship_name`, `event_time`,
       CAST('DARK' AS STRING), 40,
       `lat`, `lon`, `distance_km`, `last_sog`, `replay`
FROM `dark_events`
UNION ALL
SELECT 0, CAST('-' AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), `window_time`,
       CAST('HEARTBEAT' AS STRING), 0,
       CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), FALSE
FROM TABLE(TUMBLE(TABLE `vessel_fixes`, DESCRIPTOR(`time_utc`), INTERVAL '30' SECONDS))
GROUP BY `window_start`, `window_end`, `window_time`;
