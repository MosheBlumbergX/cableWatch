# ---------------------------------------------------------------------------
# The raw topic is the one table Flink does not create: its schema carries the
# data contract (CEL rules), which the producer's serializer enforces before a
# record is sent. Failures go to ais_position_dlq instead of the topic.
# Flink sees the topic as a table automatically; 00_alter_raw.sql adds the
# event-time watermark.
# ---------------------------------------------------------------------------

locals {
  kafka_rest = {
    rest_endpoint = confluent_kafka_cluster.cw.rest_endpoint
    key           = confluent_api_key.kafka.id
    secret        = confluent_api_key.kafka.secret
  }
}

resource "confluent_kafka_topic" "raw" {
  topic_name       = "ais_position_raw"
  partitions_count = 3
  rest_endpoint    = local.kafka_rest.rest_endpoint
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = tostring(24 * 3600 * 1000)
  }
  kafka_cluster {
    id = confluent_kafka_cluster.cw.id
  }
  credentials {
    key    = local.kafka_rest.key
    secret = local.kafka_rest.secret
  }
}

resource "confluent_kafka_topic" "dlq" {
  topic_name       = "ais_position_dlq"
  partitions_count = 1
  rest_endpoint    = local.kafka_rest.rest_endpoint
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = tostring(7 * 24 * 3600 * 1000)
  }
  kafka_cluster {
    id = confluent_kafka_cluster.cw.id
  }
  credentials {
    key    = local.kafka_rest.key
    secret = local.kafka_rest.secret
  }
}

resource "confluent_schema" "raw_value" {
  subject_name = "${confluent_kafka_topic.raw.topic_name}-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/ais_position_raw.avsc")

  # AIS uses 91 latitude, 181 longitude and 102.3 knots for "not available".
  ruleset {
    domain_rules {
      name       = "latitude_in_range"
      kind       = "CONDITION"
      type       = "CEL"
      mode       = "WRITE"
      expr       = "message.lat >= -90.0 && message.lat <= 90.0"
      on_success = "NONE"
      on_failure = "DLQ"
      params     = { "dlq.topic" = confluent_kafka_topic.dlq.topic_name }
    }
    domain_rules {
      name       = "longitude_in_range"
      kind       = "CONDITION"
      type       = "CEL"
      mode       = "WRITE"
      expr       = "message.lon >= -180.0 && message.lon <= 180.0"
      on_success = "NONE"
      on_failure = "DLQ"
      params     = { "dlq.topic" = confluent_kafka_topic.dlq.topic_name }
    }
    domain_rules {
      name       = "mmsi_is_a_ship"
      kind       = "CONDITION"
      type       = "CEL"
      mode       = "WRITE"
      expr       = "message.mmsi >= 100000000 && message.mmsi <= 999999999"
      on_success = "NONE"
      on_failure = "DLQ"
      params     = { "dlq.topic" = confluent_kafka_topic.dlq.topic_name }
    }
    domain_rules {
      name       = "speed_available"
      kind       = "CONDITION"
      type       = "CEL"
      mode       = "WRITE"
      expr       = "message.sog >= 0.0 && message.sog < 102.2"
      on_success = "NONE"
      on_failure = "DLQ"
      params     = { "dlq.topic" = confluent_kafka_topic.dlq.topic_name }
    }
  }

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.schema_registry.id
    secret = confluent_api_key.schema_registry.secret
  }
}
