-- State transitions worth telling a human about: WATCH, TRIGGER (ALERT) and RESOLVE,
-- enriched with the vessel's static data and a templated two-sentence brief.
CREATE TABLE IF NOT EXISTS `incidents` (
  `mmsi`        INT    NOT NULL,
  `cable_id`    STRING NOT NULL,
  `event_time`  TIMESTAMP_LTZ(3) NOT NULL,
  `action`      STRING,
  `state`       STRING,
  `prev_state`  STRING,
  `score`       INT,
  `signal`      STRING,
  `cable_name`  STRING,
  `vessel_name` STRING,
  `vessel_type` STRING,
  `imo`         INT,
  `destination` STRING,
  `lat`         DOUBLE,
  `lon`         DOUBLE,
  `sog`         DOUBLE,
  `distance_km` DOUBLE,
  `summary`     STRING,
  `brief`       STRING,
  `prompt`      STRING,
  `watch_reason` STRING,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '10' SECOND
)
DISTRIBUTED BY HASH(`mmsi`, `cable_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all',
  'kafka.retention.time' = '30 d'
);
