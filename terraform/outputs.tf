output "environment_id" {
  value = confluent_environment.cw.id
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.cw.id
}

output "flink_compute_pool_id" {
  value = confluent_flink_compute_pool.cw.id
}

output "bootstrap_servers" {
  value = replace(confluent_kafka_cluster.cw.bootstrap_endpoint, "SASL_SSL://", "")
}

output "kafka_api_key" {
  value = confluent_api_key.kafka.id
}

output "kafka_api_secret" {
  value     = confluent_api_key.kafka.secret
  sensitive = true
}

output "schema_registry_url" {
  value = data.confluent_schema_registry_cluster.sr.rest_endpoint
}

output "schema_registry_api_key" {
  value = confluent_api_key.schema_registry.id
}

output "schema_registry_api_secret" {
  value     = confluent_api_key.schema_registry.secret
  sensitive = true
}

output "flink_statements" {
  value = local.flink_statement_names
}

output "ntfy_topic" {
  description = "Subscribe in the ntfy app (or open the URL) to receive incident notifications."
  value       = "${var.ntfy_server}/${local.ntfy_topic}"
}

output "console_url" {
  value = "https://confluent.cloud/environments/${confluent_environment.cw.id}/flink/statements"
}

output "lineage_url" {
  value = "https://confluent.cloud/environments/${confluent_environment.cw.id}/clusters/${confluent_kafka_cluster.cw.id}/stream-lineage/stream/topic/incidents"
}

output "dashboard_url" {
  value = local.dashboard_url
}

output "app_instance_id" {
  description = "Shell access: aws ssm start-session --target <id>"
  value       = aws_instance.app.id
}
