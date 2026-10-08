# -*- coding: utf-8 -*-

import json
from kafka import KafkaProducer
from datetime import datetime, timezone

# Configuration
from kafka_config import BOOTSTRAP_SERVERS, USERNAME, PASSWORD
TOPIC="imat3a_SOL_BigDaddyks"
SYMBOL = "SOLUSD"

# Create the KafkaProducer
PRODUCER = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8")
)

def ms_to_utc_z(ms: int) -> str:
    """
    Convert epoch milliseconds to a UTC string, e.g.:
    2026-03-09T11:21:00Z
    """
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def produce(data) -> None:

    # Crea el mensaje
    key = SYMBOL
    value = {
        'symbol': data["s"],
        '@timestamp': ms_to_utc_z(data["T"]),
        'close': data["c"],
        'volume': data["v"],
    }

    # Muestra el mensaje a enviar
    print("Mensaje a enviar: ", key + " " + str(value))

    # Send the message
    PRODUCER.send(topic=TOPIC, key=key, value=value)
    PRODUCER.flush()
    # PRODUCER.close()





  