-- A vessel that stopped transmitting within 10 km of a cable. event_time = last fix + 30 min.
CREATE TABLE IF NOT EXISTS `dark_events` (
  `mmsi`         INT    NOT NULL,
  `cable_id`     STRING NOT NULL,
  `cable_name`   STRING,
  `ship_name`    STRING,
  `event_time`   TIMESTAMP_LTZ(3) NOT NULL,
  `last_seen`    TIMESTAMP_LTZ(3),
  `fixes`        BIGINT,
  `last_sog`     DOUBLE,
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
