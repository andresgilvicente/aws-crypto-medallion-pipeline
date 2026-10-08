# Kafka CLI Cheat Sheet

Minimal commands for operating the SASL-secured Kafka broker used by the streaming layer.

- Auth: `SASL_PLAINTEXT` + `PLAIN`
- Credentials are read from the environment — never commit them (see `.env.example`).

## 1) Set up a local Kafka client

```bash
sudo yum install java-17-amazon-corretto -y
cd ~ && curl -L -O https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz

export KAFKA_HOME="$HOME/kafka_2.13-3.9.2"
export BOOTSTRAP="$KAFKA_BOOTSTRAP_SERVERS"
```

## 2) Authentication config

`client.properties` is git-ignored. Generate it from your environment:

```bash
cat > client.properties <<EOF2
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$KAFKA_USERNAME" password="$KAFKA_PASSWORD";
EOF2
```

## 3) Common operations

```bash
# List topics
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --list

# Create a topic
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties \
  --create --topic imat3a_test --partitions 1 --replication-factor 1

# Describe a topic
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties \
  --describe --topic imat3a_test

# Produce a keyed message (key,value)
echo '1,Hello' | $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" \
  --producer.config client.properties --topic imat3a_test \
  --property parse.key=true --property key.separator=,

# Consume messages
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --consumer.config client.properties --topic imat3a_test \
  --property print.key=true --consumer-property group.id=imat3a_group1

# List consumer groups / inspect lag
$KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties --list
$KAFKA_HOME/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties \
  --describe --group imat3a_group1

# Delete a topic
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config client.properties \
  --delete --topic imat3a_test
```
