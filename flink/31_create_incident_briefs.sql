-- Incidents with an LLM-written brief (only when an OpenAI key is configured).
CREATE TABLE IF NOT EXISTS `incident_briefs` (
  `mmsi`           INT    NOT NULL,
  `cable_id`       STRING NOT NULL,
  `event_time`     TIMESTAMP_LTZ(3) NOT NULL,
  `action`         STRING,
  `state`          STRING,
  `score`          INT,
  `signal`         STRING,
  `cable_name`     STRING,
  `vessel_name`    STRING,
  `lat`            DOUBLE,
  `lon`            DOUBLE,
  `replay`         BOOLEAN,
  `summary`        STRING,
  `brief`          STRING,
  `template_brief` STRING
)
DISTRIBUTED BY HASH(`mmsi`, `cable_id`) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'append',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all',
  'kafka.retention.time' = '30 d'
);
