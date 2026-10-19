CREATE TABLE IF NOT EXISTS `vessel_fixes` (
  `mmsi`        INT              NOT NULL,
  `time_utc`    TIMESTAMP_LTZ(3) NOT NULL,
  `lat`         DOUBLE           NOT NULL,
  `lon`         DOUBLE           NOT NULL,
  `sog`         DOUBLE           NOT NULL,
  `cog`         DOUBLE,
  `heading`     INT,
  `nav_status`  INT              NOT NULL,
  `h3_r7`       STRING           NOT NULL,
  `ship_name`   STRING,
  `replay`      BOOLEAN          NOT NULL,
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
