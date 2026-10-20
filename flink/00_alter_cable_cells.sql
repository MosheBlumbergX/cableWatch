-- The geography is valid for all time, so its watermark is a century ahead and
-- never holds back the temporal joins (which wait for the slower side). With
-- the default watermark, every (re)load of cable_cells pins the joins to the
-- loader's 2020 timestamps, and the whole pipeline downstream freezes until
-- idleness detection notices, if it does.
--
-- The statements that join against it run with idleness detection off (see
-- statement_properties in terraform/flink.tf); otherwise an idle vessel_fixes
-- would hand the join this far-future watermark.
ALTER TABLE `cable_cells` MODIFY WATERMARK FOR `$rowtime` AS `$rowtime` + INTERVAL '36500' DAY(5);
