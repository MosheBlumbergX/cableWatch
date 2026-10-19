variable "environment_name" {
  description = "Confluent Cloud environment display name (also used as a prefix)."
  type        = string
  default     = "cablewatch"
}

variable "cluster_name" {
  description = "Kafka cluster display name (also the Flink database name)."
  type        = string
  default     = "cablewatch-cluster"
}

variable "cloud" {
  type    = string
  default = "AWS"
}

variable "region" {
  description = "Confluent Cloud region for Kafka and Flink."
  type        = string
  default     = "eu-central-1"
}

variable "flink_max_cfu" {
  description = "Upper bound for the Flink compute pool. You pay only for CFUs actually used."
  type        = number
  default     = 20
}

variable "tableflow_enabled" {
  description = "Materialise vessel_fixes, vessel_risk and incidents as Iceberg tables."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Data source and outcomes. Secrets come from .env via TF_VAR_* (see Makefile).
# ---------------------------------------------------------------------------
variable "aisstream_api_key" {
  description = "aisstream.io API key (AISSTREAM_API_KEY in .env)."
  type        = string
  sensitive   = true
}

variable "ntfy_server" {
  description = "ntfy server the HTTP Sink posts to."
  type        = string
  default     = "https://ntfy.sh"
}

variable "ntfy_topic" {
  description = "ntfy topic for incident notifications. Empty = a random cablewatch-<hex> topic."
  type        = string
  default     = ""
}

variable "pagerduty_routing_key" {
  description = "PagerDuty Events API v2 integration key. Empty = no PagerDuty connector. Note: PagerDuty wants it in the request body, so it is part of the pagerduty_events records."
  type        = string
  default     = ""
  sensitive   = true
}

variable "slack_webhook_url" {
  description = "Slack incoming webhook URL. Empty = no Slack connector."
  type        = string
  default     = ""
  sensitive   = true
}

variable "openai_api_key" {
  description = "OpenAI API key for ML_PREDICT incident briefs. Empty = templated briefs only."
  type        = string
  default     = ""
  sensitive   = true
}

variable "openai_model" {
  type    = string
  default = "gpt-4o-mini"
}

# ---------------------------------------------------------------------------
# AWS hosting
# ---------------------------------------------------------------------------
variable "aws_profile" {
  description = "AWS CLI profile used to deploy the dashboard. Null = default credential chain (AWS_PROFILE, SSO, ...)."
  type        = string
  default     = null
}

variable "aws_region" {
  description = "AWS region for the dashboard host; keep it next to the Kafka cluster."
  type        = string
  default     = "eu-central-1"
}

variable "aws_instance_type" {
  type    = string
  default = "t4g.small"
}

variable "aws_tags" {
  description = "Extra tags for every AWS resource, e.g. ones your organisation's tagging policy requires."
  type        = map(string)
  default     = {}
}

variable "repo_url" {
  description = "Source repository linked from the dashboard, e.g. https://github.com/you/cableWatch. Empty hides the link."
  type        = string
  default     = ""
}

variable "replay_cooldown_minutes" {
  description = "Minimum time between replays started from the dashboard button."
  type        = number
  default     = 20
}
