CREATE TABLE IF NOT EXISTS `corridor_fixes` (
  `mmsi`        INT              NOT NULL,
  `cable_id`    STRING           NOT NULL,
  `time_utc`    TIMESTAMP_LTZ(3) NOT NULL,
  `lat`         DOUBLE,
  `lon`         DOUBLE,
  `sog`         DOUBLE,
  `cog`         DOUBLE,
  `heading`     INT,
  `nav_status`  INT,
  `ship_name`   STRING,
  `replay`      BOOLEAN,
  `cable_name`  STRING,
  `kind`        STRING,
  `distance_km` DOUBLE,
  WATERMARK FOR `time_utc` AS `time_utc` - INTERVAL '10' SECOND
)
DISTRIBUTED BY HASH(`mmsi`) INTO 3 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all',
  'kafka.retention.time' = '7 d'
);
