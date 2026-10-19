-- One row per 20 minutes a vessel spends nearly stationary over a cable corridor.
CREATE TABLE IF NOT EXISTS `loiter_events` (
  `mmsi`         INT    NOT NULL,
  `cable_id`     STRING NOT NULL,
  `cable_name`   STRING,
  `ship_name`    STRING,
  `event_time`   TIMESTAMP_LTZ(3) NOT NULL,
  `window_start` TIMESTAMP(3),
  `fixes`        BIGINT,
  `max_sog`      DOUBLE,
  `lat`          DOUBLE,
  `lon`          DOUBLE,
  `distance_km`  DOUBLE,
  `replay`       BOOLEAN,
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
