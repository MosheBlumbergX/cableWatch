-- The three detectors' output in one stream, with the points each signal is worth.
-- The watermark allows 3 minutes: detector rows arrive one Flink checkpoint
-- later than the heartbeat rows that share this topic.
CREATE TABLE IF NOT EXISTS `risk_signals` (
  `mmsi`        INT    NOT NULL,
  `cable_id`    STRING NOT NULL,
  `cable_name`  STRING,
  `ship_name`   STRING,
  `event_time`  TIMESTAMP_LTZ(3) NOT NULL,
  `signal`      STRING,
  `points`      INT,
  `lat`         DOUBLE,
  `lon`         DOUBLE,
  `distance_km` DOUBLE,
  `sog`         DOUBLE,
  `replay`      BOOLEAN,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '3' MINUTE
)
DISTRIBUTED BY HASH(`mmsi`, `cable_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all',
  'kafka.retention.time' = '30 d'
);
