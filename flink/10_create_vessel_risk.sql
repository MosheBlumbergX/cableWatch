-- Latest score and state per vessel and cable (compacted; Tableflow upsert table).
CREATE TABLE IF NOT EXISTS `vessel_risk` (
  `mmsi`         INT    NOT NULL,
  `cable_id`     STRING NOT NULL,
  `cable_name`   STRING,
  `ship_name`    STRING,
  `updated_at`   TIMESTAMP_LTZ(3),
  `score`        INT,
  `state`        STRING,
  `last_signal`  STRING,
  `loiter_count` INT,
  `drag_count`   INT,
  `dark_count`   INT,
  `lat`          DOUBLE,
  `lon`          DOUBLE,
  PRIMARY KEY (`mmsi`, `cable_id`) NOT ENFORCED
)
DISTRIBUTED BY HASH(`mmsi`, `cable_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all'
);
