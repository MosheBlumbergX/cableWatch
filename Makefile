# Loads CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET / AISSTREAM_API_KEY
# (and the optional outcome keys) from .env for Terraform.
include .env
export

export TF_VAR_aisstream_api_key     = $(AISSTREAM_API_KEY)
export TF_VAR_openai_api_key        = $(OPENAI_API_KEY)
export TF_VAR_pagerduty_routing_key = $(PAGERDUTY_ROUTING_KEY)
export TF_VAR_slack_webhook_url     = $(SLACK_WEBHOOK_URL)

PY ?= python3

.PHONY: help infra url status env producer dashboard cables destroy

help:
	@echo "make infra      provision Confluent Cloud + AWS hosting, print the dashboard URL"
	@echo "make url        print the dashboard and ntfy URLs"
	@echo "make status     show Flink statement status"
	@echo "make env        write app/.env.app from Terraform outputs, for running the app locally"
	@echo "make producer   run the AIS producer locally"
	@echo "make dashboard  run the dashboard locally on http://localhost:8000"
	@echo "make cables     rebuild app/data from OpenStreetMap"
	@echo "make destroy    tear everything down (Confluent + AWS)"

infra:
	cd terraform && terraform init -upgrade && terraform apply -auto-approve
	@$(MAKE) -s url

url:
	@cd terraform && echo "dashboard: $$(terraform output -raw dashboard_url)" && echo "ntfy:      $$(terraform output -raw ntfy_topic)"

status:
	./scripts/flink-status.sh

env:
	@cd terraform && { \
	  echo "BOOTSTRAP_SERVERS=$$(terraform output -raw bootstrap_servers)"; \
	  echo "KAFKA_API_KEY=$$(terraform output -raw kafka_api_key)"; \
	  echo "KAFKA_API_SECRET=$$(terraform output -raw kafka_api_secret)"; \
	  echo "SCHEMA_REGISTRY_URL=$$(terraform output -raw schema_registry_url)"; \
	  echo "SCHEMA_REGISTRY_API_KEY=$$(terraform output -raw schema_registry_api_key)"; \
	  echo "SCHEMA_REGISTRY_API_SECRET=$$(terraform output -raw schema_registry_api_secret)"; \
	  echo "AISSTREAM_API_KEY=$(AISSTREAM_API_KEY)"; \
	  echo "NTFY_URL=$$(terraform output -raw ntfy_topic)"; \
	  echo "CONSOLE_URL=$$(terraform output -raw console_url)"; \
	  echo "LINEAGE_URL=$$(terraform output -raw lineage_url)"; \
	} > ../app/.env.app && chmod 600 ../app/.env.app && echo "wrote app/.env.app"

producer:
	cd app && $(PY) -u producer.py

dashboard:
	cd app && $(PY) -m uvicorn server:app --port 8000 --reload

cables:
	cd tools && $(PY) build_cables.py

destroy:
	cd terraform && terraform destroy -auto-approve
