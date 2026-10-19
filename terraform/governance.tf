# ---------------------------------------------------------------------------
# Tableflow: vessel tracks, risk state and incidents as Iceberg tables in
# Confluent-managed storage, for replay and analysis with any Iceberg engine.
# ---------------------------------------------------------------------------
resource "confluent_api_key" "tableflow" {
  display_name = "${var.environment_name}-tableflow"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }
  managed_resource {
    id          = "tableflow"
    api_version = "tableflow/v1"
    kind        = "Tableflow"
    environment {
      id = confluent_environment.cw.id
    }
  }
  depends_on = [confluent_role_binding.app_env_admin]
}

resource "confluent_tableflow_topic" "table" {
  for_each = var.tableflow_enabled ? toset(["vessel_fixes", "vessel_risk", "incidents"]) : toset([])

  display_name  = each.key
  table_formats = ["ICEBERG"]
  managed_storage {}

  # A malformed record is skipped rather than suspending the table.
  error_handling {
    mode = "SKIP"
  }

  environment {
    id = confluent_environment.cw.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.cw.id
  }
  credentials {
    key    = confluent_api_key.tableflow.id
    secret = confluent_api_key.tableflow.secret
  }

  depends_on = [confluent_flink_statement.ddl]
}

# ---------------------------------------------------------------------------
# Stream Catalog tags, so the governance story is visible in the UI:
# geo = carries positions, alerting = drives a side effect, pii_free = no
# personal data (AIS identifies ships, not people).
# ---------------------------------------------------------------------------
locals {
  sr_auth = {
    rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
    key           = confluent_api_key.schema_registry.id
    secret        = confluent_api_key.schema_registry.secret
  }

  tags = {
    geo      = "Topic carries vessel positions or cable geography"
    alerting = "Topic drives an automated outcome (HTTP Sink to ntfy / PagerDuty / Slack)"
    pii_free = "No personal data: AIS identifies vessels, not people"
  }

  topic_tags = merge(
    { for t in ["ais_position_raw", "vessel_fixes", "corridor_fixes", "cable_cells"] : t => ["geo", "pii_free"] },
    { for t in ["loiter_events", "drag_events", "dark_events", "risk_signals", "vessel_risk"] : t => ["geo", "pii_free"] },
    { for t in concat(["incidents"], local.sink_tables) : t => ["alerting", "pii_free"] },
  )

  topic_tag_pairs = merge([
    for topic, tags in local.topic_tags : { for tag in tags : "${topic}/${tag}" => { topic = topic, tag = tag } }
  ]...)
}

resource "confluent_tag" "tag" {
  for_each    = local.tags
  name        = each.key
  description = each.value

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = local.sr_auth.rest_endpoint
  credentials {
    key    = local.sr_auth.key
    secret = local.sr_auth.secret
  }
}

# The catalog picks up new topics asynchronously.
resource "time_sleep" "catalog_sync" {
  create_duration = "90s"
  depends_on      = [confluent_flink_statement.ddl, confluent_kafka_topic.raw]
}

resource "confluent_tag_binding" "topic" {
  for_each = local.topic_tag_pairs

  tag_name    = confluent_tag.tag[each.value.tag].name
  entity_name = "${data.confluent_schema_registry_cluster.sr.id}:${confluent_kafka_cluster.cw.id}:${each.value.topic}"
  entity_type = "kafka_topic"

  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = local.sr_auth.rest_endpoint
  credentials {
    key    = local.sr_auth.key
    secret = local.sr_auth.secret
  }

  depends_on = [time_sleep.catalog_sync]
}
