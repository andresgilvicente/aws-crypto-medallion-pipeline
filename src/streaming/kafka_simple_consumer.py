# -*- coding: utf-8 -*-

import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Configuration
from kafka_config import BOOTSTRAP_SERVERS, USERNAME, PASSWORD, GROUP_ID
from kafka_config import TOPIC_RAW as TOPIC_S5_1, TOPIC_VWAP as TOPIC_S5_2

# Create the KafkaConsumer
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

def main() -> None:

    # Subscribe to both topics
    # CONSUMER.assign([TopicPartition(TOPIC_S5, 0)])
    CONSUMER.subscribe([TOPIC_S5_1, TOPIC_S5_2])

    while True:

        # Read messages
        records = CONSUMER.poll(timeout_ms=3600.0)

        # Process messages
        for topic_partition, consumer_records in records.items():
            topic_name = topic_partition.topic

            for consumer_record in consumer_records:

                if topic_name == TOPIC_S5_1: 
                    print("-" * 40)
                    print("key:       " + str(consumer_record.key))
                    print("value:     " + str(consumer_record.value))
                    print("offset:    " + str(consumer_record.offset))
                    print("timestamp: " + str(consumer_record.timestamp))
                    print("-" * 40)

                elif topic_name == TOPIC_S5_2:
                   
                    data = consumer_record.value
                    print("-" * 40)
                    print(f"Symbol:  {data.get('symbol')}")
                    print(f"Window:  {data.get('window_start')} to {data.get('window_end')}")
                    print(f"VWAP:    {data.get('vwap')}")
                    print(f"data:    {data}")
                    print("-" * 40)
                    
    # Close the consumer
    # CONSUMER.close()

if __name__ == "__main__":
    main()
    


    
































################





