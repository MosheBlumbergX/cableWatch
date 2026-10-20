locals {
  sql_dir = "${path.module}/../flink"

  flink_properties = {
    "sql.current-catalog"  = confluent_environment.cw.display_name
    "sql.current-database" = confluent_kafka_cluster.cw.display_name
  }

  # Feature flags: whether a key is set is not itself a secret.
  llm_enabled       = nonsensitive(var.openai_api_key != "")
  pagerduty_enabled = nonsensitive(var.pagerduty_routing_key != "")
  slack_enabled     = nonsensitive(var.slack_webhook_url != "")

  # Payload builders read the LLM-written briefs when there are any.
  incident_source = local.llm_enabled ? "incident_briefs" : "incidents"
  sink_tables = concat(
    ["ntfy_events"],
    local.pagerduty_enabled ? ["pagerduty_events"] : [],
    local.slack_enabled ? ["slack_events"] : [],
  )

  # Tables, topics and schemas. Statement names double as the Flink job names.
  ddl = merge(
    {
      "cw-ddl-vessel-fixes"   = file("${local.sql_dir}/01_create_vessel_fixes.sql")
      "cw-ddl-cable-cells"    = file("${local.sql_dir}/02_create_cable_cells.sql")
      "cw-ddl-vessel-static"  = file("${local.sql_dir}/03_create_vessel_static.sql")
      "cw-ddl-corridor-fixes" = file("${local.sql_dir}/04_create_corridor_fixes.sql")
      "cw-ddl-loiter-events"  = file("${local.sql_dir}/05_create_loiter_events.sql")
      "cw-ddl-drag-events"    = file("${local.sql_dir}/06_create_drag_events.sql")
      "cw-ddl-dark-events"    = file("${local.sql_dir}/07_create_dark_events.sql")
      "cw-ddl-risk-signals"   = file("${local.sql_dir}/08_create_risk_signals.sql")
      "cw-ddl-risk-updates"   = file("${local.sql_dir}/09_create_risk_updates.sql")
      "cw-ddl-vessel-risk"    = file("${local.sql_dir}/10_create_vessel_risk.sql")
      "cw-ddl-incidents"      = file("${local.sql_dir}/11_create_incidents.sql")
    },
    { for t in local.sink_tables :
      "cw-ddl-${replace(t, "_", "-")}" => templatefile("${local.sql_dir}/12_create_http_sink_topics.sql.tftpl", { table = t })
    },
    local.llm_enabled ? {
      "cw-ddl-incident-briefs" = file("${local.sql_dir}/31_create_incident_briefs.sql")
    } : {},
  )

  # Long-running pipelines. They only read topics, so they can start in any order.
  dml = merge(
    {
      "cw-clean-dedupe"       = file("${local.sql_dir}/20_insert_vessel_fixes.sql")
      "cw-geofence-join"      = file("${local.sql_dir}/21_insert_corridor_fixes.sql")
      "cw-detect-loiter"      = file("${local.sql_dir}/22_insert_loiter_events.sql")
      "cw-detect-anchor-drag" = file("${local.sql_dir}/23_insert_drag_events.sql")
      "cw-detect-dark"        = file("${local.sql_dir}/24_insert_dark_events.sql")
      "cw-risk-signals"       = file("${local.sql_dir}/25_insert_risk_signals.sql")
      "cw-risk-score" = templatefile("${local.sql_dir}/26_insert_risk_score.sql.tftpl", {
        flags  = var.watch_flags
        points = var.watch_points
        repeat = var.watch_repeat_signals
      })
      "cw-vessel-risk" = file("${local.sql_dir}/28_insert_vessel_risk.sql")
      "cw-incidents"   = file("${local.sql_dir}/29_insert_incidents.sql")
    },
    local.llm_enabled ? {
      "cw-incident-briefs" = file("${local.sql_dir}/32_insert_incident_briefs.sql")
    } : {},
  )

  # Payload builders for the HTTP Sink connectors; they embed the dashboard URL.
  dml_payloads = merge(
    {
      "cw-payload-ntfy" = templatefile("${local.sql_dir}/40_insert_ntfy_events.sql.tftpl", {
        ntfy_topic      = local.ntfy_topic
        dashboard_url   = local.dashboard_url
        source          = local.incident_source
        include_service = var.notify_service_vessels ? "TRUE" : "FALSE"
      })
    },
    local.pagerduty_enabled ? {
      "cw-payload-pagerduty" = templatefile("${local.sql_dir}/41_insert_pagerduty_events.sql.tftpl", {
        routing_key     = var.pagerduty_routing_key
        dashboard_url   = local.dashboard_url
        source          = local.incident_source
        include_service = var.notify_service_vessels ? "TRUE" : "FALSE"
      })
    } : {},
    local.slack_enabled ? {
      "cw-payload-slack" = templatefile("${local.sql_dir}/42_insert_slack_events.sql.tftpl", {
        dashboard_url   = local.dashboard_url
        source          = local.incident_source
        include_service = var.notify_service_vessels ? "TRUE" : "FALSE"
      })
    } : {},
  )

  # Per-statement settings.
  #  - Dedupe keys include the position, so without a TTL its state would grow
  #    with every fix ever received.
  #  - The cable lookups join against cable_cells, whose watermark is a century
  #    ahead (00_alter_cable_cells.sql). Idleness detection must be off for them:
  #    if vessel_fixes were ever marked idle (as it is before the producer
  #    starts), the join's watermark would jump to the cable_cells one, and every
  #    later fix would be dropped as late.
  no_idleness = { "sql.tables.scan.idle-timeout" = "0 ms" }
  statement_properties = {
    "cw-clean-dedupe"  = { "sql.state-ttl" = "10 min" }
    "cw-geofence-join" = local.no_idleness
    "cw-detect-dark"   = local.no_idleness
  }

  flink_statement_names = concat(["cw-ddl-raw-watermark", "cw-ddl-cable-cells-watermark"], keys(local.ddl), keys(local.dml), keys(local.dml_payloads))
}

resource "random_id" "ntfy" {
  byte_length = 6
}

locals {
  ntfy_topic    = var.ntfy_topic != "" ? var.ntfy_topic : "cablewatch-${random_id.ntfy.hex}"
  dashboard_url = "https://${aws_cloudfront_distribution.app.domain_name}"
}

# ---------------------------------------------------------------------------
# Statements
# ---------------------------------------------------------------------------
resource "confluent_flink_statement" "ddl" {
  for_each = local.ddl

  statement_name = each.key
  statement      = each.value
  properties     = local.flink_properties

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.cw.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.cw.id
  }
  principal {
    id = confluent_service_account.app.id
  }
  rest_endpoint = data.confluent_flink_region.cw.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [confluent_api_key.kafka, confluent_api_key.schema_registry]
}

# Watermark changes that CREATE TABLE cannot express: the raw topic is created by
# Terraform, and $rowtime watermarks can only be set with ALTER.
resource "confluent_flink_statement" "ddl_alter" {
  for_each = {
    "cw-ddl-raw-watermark"         = file("${local.sql_dir}/00_alter_raw.sql")
    "cw-ddl-cable-cells-watermark" = file("${local.sql_dir}/00_alter_cable_cells.sql")
  }

  statement_name = each.key
  statement      = each.value
  properties     = local.flink_properties

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.cw.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.cw.id
  }
  principal {
    id = confluent_service_account.app.id
  }
  rest_endpoint = data.confluent_flink_region.cw.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [confluent_schema.raw_value, confluent_flink_statement.ddl]
}

moved {
  from = confluent_flink_statement.raw_watermark
  to   = confluent_flink_statement.ddl_alter["cw-ddl-raw-watermark"]
}

# ---------------------------------------------------------------------------
# Flink AI (only with an OpenAI key): connection + model for ML_PREDICT
# ---------------------------------------------------------------------------
resource "confluent_flink_connection" "openai" {
  count = local.llm_enabled ? 1 : 0

  display_name = "openai-conn"
  type         = "OPENAI"
  endpoint     = "https://api.openai.com/v1/chat/completions"
  api_key      = var.openai_api_key

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.cw.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.cw.id
  }
  principal {
    id = confluent_service_account.app.id
  }
  rest_endpoint = data.confluent_flink_region.cw.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
}

resource "confluent_flink_statement" "model" {
  count = local.llm_enabled ? 1 : 0

  statement_name = "cw-ddl-model"
  statement = templatefile("${local.sql_dir}/30_create_model.sql.tftpl", {
    connection = confluent_flink_connection.openai[0].display_name
    model      = var.openai_model
  })
  properties = local.flink_properties

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.cw.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.cw.id
  }
  principal {
    id = confluent_service_account.app.id
  }
  rest_endpoint = data.confluent_flink_region.cw.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }
}

resource "confluent_flink_statement" "dml" {
  for_each = local.dml

  statement_name = each.key
  statement      = each.value
  properties     = merge(local.flink_properties, lookup(local.statement_properties, each.key, {}))

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.cw.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.cw.id
  }
  principal {
    id = confluent_service_account.app.id
  }
  rest_endpoint = data.confluent_flink_region.cw.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [confluent_flink_statement.ddl_alter, confluent_flink_statement.ddl, confluent_flink_statement.model]
}

resource "confluent_flink_statement" "dml_payloads" {
  for_each = local.dml_payloads

  statement_name = each.key
  statement      = each.value
  properties     = local.flink_properties

  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.cw.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.cw.id
  }
  principal {
    id = confluent_service_account.app.id
  }
  rest_endpoint = data.confluent_flink_region.cw.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  depends_on = [confluent_flink_statement.ddl, confluent_flink_statement.dml]
}
