"""Avro serializers bound to the schemas Flink (or Terraform) registered.

Nothing here registers a schema: the producer always writes with the latest
version, so the data contract rules attached to that version are enforced.
"""
import json

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.rules.cel.cel_executor import CelExecutor
from confluent_kafka.schema_registry.rules.dlq.dlq_action import DlqAction
from confluent_kafka.serialization import MessageField, SerializationContext

import config

_rules_registered = False


def register_rules() -> None:
    """CEL conditions plus the DLQ action, which tees failing records to the DLQ topic."""
    global _rules_registered
    if not _rules_registered:
        CelExecutor.register()
        DlqAction.register({**config.kafka_conf(), "dlq.auto.flush": "false"})
        _rules_registered = True


class TopicSerializer:
    """Serializes key and value for one topic. A record key schema gets a dict; anything else is sent as-is."""

    def __init__(self, sr: SchemaRegistryClient, topic: str, avro_key: bool = True):
        self.topic = topic
        conf = {"auto.register.schemas": False, "use.latest.version": True}
        value_schema = sr.get_latest_version(f"{topic}-value").schema
        self._value = AvroSerializer(sr, value_schema.schema_str, conf=conf)
        self._value_ctx = SerializationContext(topic, MessageField.VALUE)
        self._key = None
        if avro_key:
            key_schema = sr.get_latest_version(f"{topic}-key").schema
            parsed = json.loads(key_schema.schema_str)
            self._key_fields = [f["name"] for f in parsed["fields"]] if isinstance(parsed, dict) and parsed.get("type") == "record" else None
            self._key = AvroSerializer(sr, key_schema.schema_str, conf=conf)
            self._key_ctx = SerializationContext(topic, MessageField.KEY)

    def key(self, record: dict) -> bytes:
        if self._key is None:
            raise ValueError("no key schema")
        if self._key_fields is None:
            raise ValueError("primitive key schemas are not used in this demo")
        return self._key({k: record[k] for k in self._key_fields}, self._key_ctx)

    def value(self, record: dict) -> bytes:
        return self._value(record, self._value_ctx)


def registry() -> SchemaRegistryClient:
    return SchemaRegistryClient(config.schema_registry_conf())
