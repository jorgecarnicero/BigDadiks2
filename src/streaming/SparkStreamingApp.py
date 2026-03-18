from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, struct, to_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. INICIAR SPARK
spark = SparkSession.builder \
    .appName("Calculo_VWAP_SOL") \
    .getOrCreate()

# Reducimos el spam en la consola
spark.sparkContext.setLogLevel("WARN")

BOOTSTRAP_SERVERS = "51.49.235.244:9092"
TOPIC_IN = "imat3a_SOL_BigDaddyks"
TOPIC_OUT = "imat3a_SOL_BigDaddyks_VWAP"

# IMPORTANTE: Necesitamos decirle a Spark qué forma tiene el JSON crudo que envía tu Productor
schema_entrada = StructType([
    StructField("symbol", StringType(), True),
    StructField("@timestamp", TimestampType(), True), # Spark necesita un timestamp para las ventanas
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
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

    # Los datos de Kafka vienen en binario, los pasamos a String y luego a JSON
    df_json = df_crudo.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema_entrada).alias("data")) \
        .select("data.*")

    # 3. EL CÁLCULO MÁGICO (Ventanas de 5 minutos + Fórmula VWAP)
    # Primero pre-calculamos (Precio * Volumen)
    df_precalc = df_json.withColumn("precio_x_volumen", col("close") * col("volume"))

    # Agrupamos por símbolo y por ventana de 5 minutos basándonos en el timestamp
    df_agrupado = df_precalc.groupBy(
        window(col("@timestamp"), "5 minutes"),
        col("symbol")
    ).agg(
        _sum("precio_x_volumen").alias("sum_pv"),
        _sum("volume").alias("sum_v")
    )

    # Calculamos el VWAP final: Sum(P*V) / Sum(V)
    df_vwap = df_agrupado.withColumn("vwap", col("sum_pv") / col("sum_v"))

    # 4. DARLE EL FORMATO DE SALIDA (Para cumplir la HU-7)
    # Creamos un JSON con la estructura exacta que pide la historia de usuario
    df_salida = df_vwap.select(
        col("symbol").alias("key"), # La clave de Kafka es el símbolo
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("vwap")
        )).alias("value") # El valor de Kafka es el JSON completo
    )

    # 5. ESCRIBIR EN KAFKA (El nuevo Topic)
    query = df_salida.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_OUT) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_client" password="88b8a35dca1a04da57dc5f3e";') \
        .option("checkpointLocation", "/tmp/spark_checkpoint_vwap") \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()