"""Shared Kafka connection settings, read from environment variables.

Required:
    KAFKA_BOOTSTRAP_SERVERS   e.g. "broker.example.com:9092"
    KAFKA_USERNAME
    KAFKA_PASSWORD
"""

import os


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is not set (see .env.example)")
    return value


BOOTSTRAP_SERVERS = _require("KAFKA_BOOTSTRAP_SERVERS")
USERNAME = _require("KAFKA_USERNAME")
PASSWORD = _require("KAFKA_PASSWORD")

TOPIC_RAW = "imat3a_SOL_BigDaddyks"
TOPIC_VWAP = "imat3a_SOL_BigDaddyks_VWAP"
GROUP_ID = "imat3a_SOL_BigDaddyks"
