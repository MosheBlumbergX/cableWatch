-- Every change to a vessel's score over one cable: one row per signal (running
-- score over the last hour) plus a CLEAR row when the vessel has been quiet for an hour.
CREATE TABLE IF NOT EXISTS `risk_updates` (
  `mmsi`         INT    NOT NULL,
  `cable_id`     STRING NOT NULL,
  `cable_name`   STRING,
  `ship_name`    STRING,
  `event_time`   TIMESTAMP_LTZ(3) NOT NULL,
  `signal`       STRING,
  `points`       INT,
  `score`        INT,
  `state`        STRING,
  `loiter_count` INT,
  `drag_count`   INT,
  `dark_count`   INT,
  `lat`          DOUBLE,
  `lon`          DOUBLE,
  `distance_km`  DOUBLE,
  `sog`          DOUBLE,
  `replay`       BOOLEAN,
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
