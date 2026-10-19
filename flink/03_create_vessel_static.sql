-- ShipStaticData from the producer: latest name, type and dimensions per vessel.
CREATE TABLE IF NOT EXISTS `vessel_static` (
  `mmsi`        INT NOT NULL,
  `name`        STRING,
  `call_sign`   STRING,
  `imo`         INT,
  `ship_type`   INT,
  `destination` STRING,
  `length_m`    INT,
  `beam_m`      INT,
  `draught_m`   DOUBLE,
  PRIMARY KEY (`mmsi`) NOT ENFORCED
)
DISTRIBUTED BY HASH(`mmsi`) INTO 3 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all'
);
