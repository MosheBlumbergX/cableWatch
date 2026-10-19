-- ais_position_raw is created by Terraform (its schema carries the data contract).
-- Use the AIS receive time as event time instead of the Kafka timestamp.
ALTER TABLE `ais_position_raw` MODIFY WATERMARK FOR `time_utc` AS `time_utc` - INTERVAL '10' SECOND;
