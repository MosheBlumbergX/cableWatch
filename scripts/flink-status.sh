#!/usr/bin/env bash
# Prints the phase of every Flink statement in the demo environment.
set -euo pipefail
cd "$(dirname "$0")/../terraform"
state=$(mktemp); trap 'rm -f "$state"' EXIT
terraform show -json > "$state"
q() { jq -r ".values.root_module.resources[] | select(.address==\"$1\") | .values.$2" "$state"; }
key=$(q confluent_api_key.flink id); secret=$(q confluent_api_key.flink secret)
org=$(q data.confluent_organization.main id); endpoint=$(q data.confluent_flink_region.cw rest_endpoint)
env=$(terraform output -raw environment_id)
curl -s -u "$key:$secret" "$endpoint/sql/v1/organizations/$org/environments/$env/statements?page_size=100" |
  jq -r '.data[] | [.name, .status.phase, (.status.detail // "" | .[0:120])] | @tsv' | column -t -s $'\t'
