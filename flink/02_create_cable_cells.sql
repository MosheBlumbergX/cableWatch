-- Static geography, written once by loader.py. One row per H3 resolution-7 cell
-- within 10 km of a cable; a cell near several cables lists them all. Keyed by
-- the cell alone, because a temporal join must cover the versioned table's key.
-- Its watermark is set by 00_alter_cable_cells.sql.
CREATE TABLE IF NOT EXISTS `cable_cells` (
  `h3_r7`  STRING NOT NULL,
  `cables` ARRAY<ROW<`cable_id` STRING, `cable_name` STRING, `kind` STRING, `distance_km` DOUBLE>>,
  PRIMARY KEY (`h3_r7`) NOT ENFORCED
)
DISTRIBUTED BY HASH(`h3_r7`) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'key.format'           = 'avro-registry',
  'value.format'         = 'avro-registry',
  'value.fields-include' = 'all'
);
