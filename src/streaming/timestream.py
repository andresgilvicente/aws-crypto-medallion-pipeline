#!/usr/bin/env python3
"""
Kafka -> Amazon Timestream consumer.

Listens to two topics and writes to two Timestream tables:
- imat3a_SOL_BigDaddyks         -> sol_quotes_raw_bigdaddyks (measure: close)
- imat3a_SOL_BigDaddyks_VWAP    -> sol_vwap_5m_bigdaddyks    (measure: vwap)

It consumes the messages produced by:
- kafka_simple_producer.py (close and volume per 1m candle)
- SparkStreamingApp.py (5m VWAP)

Intended to run on an EC2 instance.
"""

import json
import time
from datetime import datetime, timezone

import boto3

from kafka import KafkaConsumer

from kafka_config import BOOTSTRAP_SERVERS, USERNAME, PASSWORD, GROUP_ID

# ==========================================
# CONFIGURATION
# ==========================================
REGION = "eu-west-1"                        # AWS region
DATABASE = "imat3a_crypto_rt"               # Timestream database
QUOTES_TABLE = "sol_quotes_raw_bigdaddyks"  # Raw quotes table
VWAP_TABLE = "sol_vwap_5m_bigdaddyks"       # 5-minute VWAP table

TOPIC_S5_1 = "imat3a_SOL_BigDaddyks"       # Raw messages: close/volume per 1m candle
TOPIC_S5_2 = "imat3a_SOL_BigDaddyks_VWAP"  # Aggregated messages: VWAP and 5m window

# KafkaConsumer (key deserialized as text, value as JSON)
CONSUMER = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    group_id=GROUP_ID,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    key_deserializer=lambda v: v.decode("utf-8"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

# ==========================================
# HELPERS
# ==========================================

def iso_to_epoch_ms(value: str) -> str:
    """
    Convert an ISO-8601 string (e.g. "2026-03-25T15:15:00.000Z") to epoch
    milliseconds. The trailing 'Z' is replaced with '+00:00' so that Python
    can parse it. Timestream expects the time as a string.
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def kafka_ts_to_epoch_ms(kafka_ts_ms: int) -> str:
    """Kafka message timestamps are already epoch milliseconds; Timestream wants a string."""
    return str(int(kafka_ts_ms))


def build_quote_record(value: dict, kafka_ts_ms: int) -> dict:
    """
    Build the record for the quotes table (sol_quotes_raw_bigdaddyks).
    Expects the payload produced by kafka_simple_producer.py:
    {
        'symbol': 'SOLUSD',
        '@timestamp': '2026-03-09T11:21:00Z',
        'close': '123.45',
        'volume': '456.78'
    }
    """

    symbol = value.get("symbol", "UNKNOWN")
    # Use @timestamp when present; otherwise fall back to the Kafka timestamp
    event_time_ms = iso_to_epoch_ms(value["@timestamp"]) if "@timestamp" in value else kafka_ts_to_epoch_ms(kafka_ts_ms)

    return {
        "Dimensions": [
            {"Name": "symbol", "Value": symbol},
            {"Name": "source_topic", "Value": TOPIC_S5_1},
        ],
        "MeasureName": "close",
        "MeasureValue": str(float(value.get("close", 0.0))),
        "MeasureValueType": "DOUBLE",
        "Time": event_time_ms,
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000),
    }


def build_vwap_record(value: dict, kafka_ts_ms: int) -> dict:
    """
    Build the record for the VWAP table (sol_vwap_5m_bigdaddyks).
    Expects the payload produced by SparkStreamingApp.py:
    {
        'window_start': '2026-03-25T15:10:00Z',
        'window_end':   '2026-03-25T15:15:00Z',
        'symbol': 'SOLUSD',
        'vwap': 123.45
    }
    """

    symbol = value.get("symbol", "UNKNOWN")
    window_start = value.get("window_start")
    window_end = value.get("window_end")

    # If the window is missing, fall back to the Kafka timestamp
    time_ms = iso_to_epoch_ms(window_end) if window_end else kafka_ts_to_epoch_ms(kafka_ts_ms)

    return {
        "Dimensions": [
            {"Name": "symbol", "Value": symbol},
            {"Name": "window_start", "Value": window_start or ""},
            {"Name": "window_end", "Value": window_end or ""},
            {"Name": "source_topic", "Value": TOPIC_S5_2},
        ],
        "MeasureName": "vwap",
        "MeasureValue": str(float(value.get("vwap", 0.0))),
        "MeasureValueType": "DOUBLE",
        "Time": time_ms,
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000),
    }

# ==========================================
# MAIN
# ==========================================

def main() -> None:
    # Boto3 resolves AWS credentials automatically (env vars, profile, instance role)
    ts = boto3.client("timestream-write", region_name=REGION)

    CONSUMER.subscribe([TOPIC_S5_1, TOPIC_S5_2])

    # Main loop: read and write continuously
    while True:
        records = CONSUMER.poll(timeout_ms=1000)

        if not records:
            continue

        for topic_partition, consumer_records in records.items():
            topic_name = topic_partition.topic

            for consumer_record in consumer_records:
                value = consumer_record.value
                kafka_ts_ms = consumer_record.timestamp

                try:
                    if topic_name == TOPIC_S5_1:
                        # Raw message (close/volume) -> sol_quotes_raw_bigdaddyks only
                        quote_record = build_quote_record(value, kafka_ts_ms)

                        ts.write_records(
                            DatabaseName=DATABASE,
                            TableName=QUOTES_TABLE,
                            Records=[quote_record],
                        )

                        print("Write OK -> quotes", json.dumps(quote_record))

                    elif topic_name == TOPIC_S5_2:
                        # Aggregated message (5m VWAP) -> sol_vwap_5m_bigdaddyks only
                        vwap_record = build_vwap_record(value, kafka_ts_ms)

                        ts.write_records(
                            DatabaseName=DATABASE,
                            TableName=VWAP_TABLE,
                            Records=[vwap_record],
                        )

                        print("Write OK -> vwap", json.dumps(vwap_record))

                    else:
                        # Unknown topic: ignore it but log a warning
                        print(f"Unhandled topic: {topic_name}")

                except Exception as exc:
                    # Catch write/parse errors so the loop stays alive
                    print(f"Error processing topic {topic_name}: {exc}")

if __name__ == "__main__":
    main()
