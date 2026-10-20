-- Latest state per vessel and cable, for the map and for Tableflow.
INSERT INTO `vessel_risk`
SELECT `mmsi`, `cable_id`, `cable_name`, `ship_name`, `event_time`, `score`, `state`, `signal`,
       `loiter_count`, `drag_count`, `dark_count`, `lat`, `lon`
FROM `risk_updates`
WHERE `signal` <> 'HEARTBEAT';
