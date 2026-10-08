"""Spark Structured Streaming job: 5-minute VWAP of SOL/USD candles (Kafka -> Kafka).

Runs as an AWS Glue streaming job.
"""


import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, struct, to_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. START SPARK
spark = SparkSession.builder \
    .appName("Calculo_VWAP_SOL") \
    .getOrCreate()

# Reduce Spark console noise
spark.sparkContext.setLogLevel("WARN")

# Connection settings come from the Glue job environment (never hardcode credentials)
BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{os.environ["KAFKA_USERNAME"]}" password="{os.environ["KAFKA_PASSWORD"]}";'
)
TOPIC_IN = "imat3a_SOL_BigDaddyks"
TOPIC_OUT = "imat3a_SOL_BigDaddyks_VWAP"

# Schema of the raw JSON
schema_entrada = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", TimestampType(), True), 
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
])

def main():
    # 2. READ FROM KAFKA
    df_crudo = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_IN) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
        .option("maxOffsetsPerTrigger", 5) \
        .load()

    # Cast to string and parse JSON
    df_json = df_crudo.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema_entrada).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            col("data.@timestamp").alias("event_ts"), 
            col("data.close").alias("close"),
            col("data.volume").alias("volume"),
        )

    # 3. THE CALCULATION 
    df_precalc = df_json.withColumn("precio_x_volumen", col("close").cast("double") * col("volume").cast("double")) \
                        .withColumn("volume_num", col("volume").cast("double"))

    # Minimal watermark for near-instant results
    df_precalc = df_precalc.withWatermark("event_ts", "0 seconds")

    # 5-min window sliding every 1 min 
    df_agrupado = df_precalc.groupBy(
        window(col("event_ts"), "5 minutes", "1 minute"),
        col("symbol")
    ).agg(
        _sum("precio_x_volumen").alias("sum_pv"),
        _sum("volume_num").alias("sum_v")
    )

    # Final VWAP formula
    df_vwap = df_agrupado.filter(col("sum_v") > 0).withColumn("vwap", col("sum_pv") / col("sum_v"))

    # 4. FORMATO DE SALIDA (Para Timestream)
    df_salida = df_vwap.select(
        col("symbol").alias("key"), 
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("vwap")
        )).alias("value") 
    )

    # 5. WRITE TO KAFKA
    query = df_salida.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_OUT) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
        .option("checkpointLocation", "/tmp/spark_checkpoint_vwap_v8_opcion2") \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()