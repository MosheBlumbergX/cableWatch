terraform {
  required_version = ">= 1.5"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

# Credentials come from CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET env vars (see Makefile).
provider "confluent" {}

data "confluent_organization" "main" {}

# ---------------------------------------------------------------------------
# Environment + Stream Governance. ADVANCED is what allows data quality rules
# (the CEL checks on ais_position_raw) and business metadata tags.
# ---------------------------------------------------------------------------
resource "confluent_environment" "cw" {
  display_name = var.environment_name

  stream_governance {
    package = "ADVANCED"
  }
}

data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = confluent_environment.cw.id
  }
  depends_on = [confluent_kafka_cluster.cw]
}

# ---------------------------------------------------------------------------
# Kafka cluster
# ---------------------------------------------------------------------------
resource "confluent_kafka_cluster" "cw" {
  display_name = var.cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region
  basic {}

  environment {
    id = confluent_environment.cw.id
  }
}

# ---------------------------------------------------------------------------
# Service account used by the apps, the Flink statements and the connectors
# ---------------------------------------------------------------------------
resource "confluent_service_account" "app" {
  display_name = "${var.environment_name}-app"
  description  = "Cable Watch: producer, dashboard, Flink statements and sink connectors"
}

resource "confluent_role_binding" "app_env_admin" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.cw.resource_name
}

resource "confluent_role_binding" "app_flink_developer" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_environment.cw.resource_name
}

# Lets the statements run "as" this service account.
resource "confluent_role_binding" "app_assigner" {
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "Assigner"
  crn_pattern = "${data.confluent_organization.main.resource_name}/service-account=${confluent_service_account.app.id}"
}

resource "confluent_api_key" "kafka" {
  display_name = "${var.environment_name}-kafka"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.cw.id
    api_version = confluent_kafka_cluster.cw.api_version
    kind        = confluent_kafka_cluster.cw.kind
    environment {
      id = confluent_environment.cw.id
    }
  }
  depends_on = [confluent_role_binding.app_env_admin]
}

resource "confluent_api_key" "schema_registry" {
  display_name = "${var.environment_name}-sr"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = confluent_environment.cw.id
    }
  }
  depends_on = [confluent_role_binding.app_env_admin]
}

# ---------------------------------------------------------------------------
# Flink
# ---------------------------------------------------------------------------
resource "confluent_flink_compute_pool" "cw" {
  display_name = "${var.environment_name}-pool"
  cloud        = var.cloud
  region       = var.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.cw.id
  }
}

data "confluent_flink_region" "cw" {
  cloud  = var.cloud
  region = var.region
}

resource "confluent_api_key" "flink" {
  display_name = "${var.environment_name}-flink"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }
  managed_resource {
    id          = data.confluent_flink_region.cw.id
    api_version = data.confluent_flink_region.cw.api_version
    kind        = data.confluent_flink_region.cw.kind
    environment {
      id = confluent_environment.cw.id
    }
  }
  depends_on = [
    confluent_role_binding.app_env_admin,
    confluent_role_binding.app_flink_developer,
    confluent_role_binding.app_assigner,
  ]
}
