-- Flink AI: the LLM rewrites the facts into a two-sentence brief for the pager.
-- Resolves keep the templated text; there is nothing to explain.
INSERT INTO `incident_briefs`
SELECT
  i.`mmsi`, i.`cable_id`, i.`event_time`, i.`action`, i.`state`, i.`score`, i.`signal`,
  i.`cable_name`, i.`vessel_name`, i.`vessel_type`, i.`lat`, i.`lon`, i.`summary`,
  CASE WHEN i.`action` = 'RESOLVE' THEN i.`brief` ELSE COALESCE(p.`brief`, i.`brief`) END,
  i.`brief`
FROM `incidents` AS i,
LATERAL TABLE(ML_PREDICT('incident_writer', i.`prompt`)) AS p;
