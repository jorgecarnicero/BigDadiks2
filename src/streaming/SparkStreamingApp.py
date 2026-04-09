"""
Este script lo usamos como un Glue Job
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, struct, to_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. INICIAR SPARK
spark = SparkSession.builder \
    .appName("Calculo_VWAP_SOL") \
    .getOrCreate()

# Reducimos el spam nativo de Spark en la consola
spark.sparkContext.setLogLevel("WARN")

BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_IN = "imat3a_SOL_BigDaddyks"
TOPIC_OUT = "imat3a_SOL_BigDaddyks_VWAP"

# El esquema del JSON crudo
schema_entrada = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", TimestampType(), True), 
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
])

def main():
    # 2. LEER DE KAFKA
    df_crudo = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_IN) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_client" password="88b8a35dca1a04da57dc5f3e";') \
        .option("maxOffsetsPerTrigger", 5) \
        .load()

    # Casteo a String y parseo a JSON
    df_json = df_crudo.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema_entrada).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            col("data.@timestamp").alias("event_ts"), 
            col("data.close").alias("close"),
            col("data.volume").alias("volume"),
        )

    # 3. EL CÁLCULO 
    df_precalc = df_json.withColumn("precio_x_volumen", col("close").cast("double") * col("volume").cast("double")) \
                        .withColumn("volume_num", col("volume").cast("double"))

    # Watermark al mínimo para tener respuesta instantánea
    df_precalc = df_precalc.withWatermark("event_ts", "0 seconds")

    # Ventana de 5 min que avanza cada 1 min 
    df_agrupado = df_precalc.groupBy(
        window(col("event_ts"), "5 minutes", "1 minute"),
        col("symbol")
    ).agg(
        _sum("precio_x_volumen").alias("sum_pv"),
        _sum("volume_num").alias("sum_v")
    )

    # Fórmula VWAP final
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

    # 5. ESCRIBIR EN KAFKA
    query = df_salida.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_OUT) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_client" password="88b8a35dca1a04da57dc5f3e";') \
        .option("checkpointLocation", "/tmp/spark_checkpoint_vwap_v8_opcion2") \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()