# ---------------------------------------------------------------------------
# The automated outcome: fully managed HTTP Sink V2 connectors post the bodies
# Flink built, verbatim. No custom consumer touches the side effect. Failed
# calls land in each connector's DLQ topic.
# ---------------------------------------------------------------------------

locals {
  slack_url_parts = regex("^(https://[^/]+)(/.*)$", local.slack_enabled ? var.slack_webhook_url : "https://hooks.slack.com/none")

  http_sinks = merge(
    {
      ntfy = {
        topic    = "ntfy_events"
        base_url = var.ntfy_server
        path     = "/"
      }
    },
    local.pagerduty_enabled ? {
      pagerduty = {
        topic    = "pagerduty_events"
        base_url = "https://events.pagerduty.com"
        path     = "/v2/enqueue"
      }
    } : {},
    local.slack_enabled ? {
      slack = {
        topic    = "slack_events"
        base_url = local.slack_url_parts[0]
        path     = local.slack_url_parts[1]
        secret   = true
      }
    } : {},
  )
}

resource "confluent_connector" "http_sink" {
  for_each = local.http_sinks

  environment {
    id = confluent_environment.cw.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.cw.id
  }

  # The Slack webhook path is the credential; the others are plain paths.
  config_sensitive = lookup(each.value, "secret", false) ? { "api1.http.api.path" = each.value.path } : {}

  config_nonsensitive = merge(lookup(each.value, "secret", false) ? {} : { "api1.http.api.path" = each.value.path }, {
    "connector.class"          = "HttpSinkV2"
    "name"                     = "cablewatch-${each.key}-sink"
    "topics"                   = each.value.topic
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app.id
    "input.data.format"        = "STRING"
    "tasks.max"                = "1"

    "http.api.base.url" = each.value.base_url
    "auth.type"         = "NONE"
    "apis.num"          = "1"

    "api1.topics"                = each.value.topic
    "api1.http.request.method"   = "POST"
    "api1.request.body.format"   = "STRING"
    "api1.http.request.headers"  = "Content-Type:application/json"
    "api1.max.batch.size"        = "1"
    "api1.batch.json.as.array"   = "false"
    "api1.retry.on.status.codes" = "500-"
    "api1.max.retries"           = "5"

    "behavior.on.error"                 = "ignore"
    "errors.tolerance"                  = "all"
    "errors.deadletterqueue.topic.name" = "${each.value.topic}_dlq"
  })

  depends_on = [
    confluent_flink_statement.ddl,
    confluent_role_binding.app_env_admin,
  ]
}
