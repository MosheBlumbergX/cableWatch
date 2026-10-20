-- Anchor-drag signature: moving at a few knots while reporting "at anchor", or sideways.
CREATE TABLE IF NOT EXISTS `drag_events` (
  `mmsi`         INT    NOT NULL,
  `cable_id`     STRING NOT NULL,
  `cable_name`   STRING,
  `ship_name`    STRING,
  `event_time`   TIMESTAMP_LTZ(3) NOT NULL,
  `start_time`   TIMESTAMP_LTZ(3),
  `fixes`        BIGINT,
  `avg_sog`      DOUBLE,
  `lat`          DOUBLE,
  `lon`          DOUBLE,
  `distance_km`  DOUBLE,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
)
DISTRIBUTED BY HASH(`mmsi`) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all',
  'kafka.retention.time' = '30 d'
);
