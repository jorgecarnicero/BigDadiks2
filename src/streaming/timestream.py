#!/usr/bin/env python3
"""
Consumidor Kafka -> Amazon Timestream.

Escucha dos topics y escribe en dos tablas Timestream:
- imat3a_SOL_BigDaddyks         -> sol_quotes_raw_bigdaddyks (medida: close)
- imat3a_SOL_BigDaddyks_VWAP    -> sol_vwap_5m_bigdaddyks    (medida: vwap)

Este script se basa en los mensajes generados por:
- kafka_simple_producer.py (close y volumen por vela 1m)
- SparkStreamingApp.py (VWAP 5m)

Este script lo vamos a ejecutar en la instancia de la EC2
"""

# Importamos las librerías necesarias
import json                  # Para formatear la salida por pantalla de forma legible (pretty print)
import time                  # Para generar un timestamp actual para la versión del registro
from datetime import datetime, timezone # Para el manejo y conversión de fechas

import boto3                 # El SDK de AWS para Python. Nos permite interactuar con los servicios de AWS (como Timestream)

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# ==========================================
# PARÁMETROS DE CONFIGURACIÓN
# ==========================================
# Aquí definimos las variables globales que le dicen al script dónde escribir y qué datos usar.
REGION = "eu-west-1"          # Región de AWS
DATABASE = "imat3a_crypto_rt" # Base de datos en Timestream
QUOTES_TABLE = "sol_quotes_raw_bigdaddyks"  # Tabla para las cotizaciones en crudo
VWAP_TABLE = "sol_vwap_5m_bigdaddyks"       # Tabla para el VWAP 5m

# ==========================================
# Configuración - Parámetros consumer
# =========================================
BOOTSTRAP_SERVERS = "51.49.235.244:9092"
USERNAME = "kafka_client"
PASSWORD = "88b8a35dca1a04da57dc5f3e"
TOPIC_S5_1 = "imat3a_SOL_BigDaddyks"       # Mensajes crudos: close/volume por vela 1m
TOPIC_S5_2 = "imat3a_SOL_BigDaddyks_VWAP"  # Mensajes agregados: vwap y ventana 5m
GROUP_ID = "imat3a_SOL_BigDaddyks"

# Creamos el KafkaConsumer (deserializa clave como texto y valor como JSON)
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
# FUNCIONES AUXILIARES
# ==========================================

def now_epoch_ms() -> str:
    """
    Devuelve el momento actual exacto en milisegundos desde la época Unix (1 enero 1970).
    Timestream requiere que las marcas de tiempo (timestamps) se envíen en este formato numérico o en segundos.
    """
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))

def iso_to_epoch_ms(value: str) -> str:
    """
    Convierte una fecha en formato texto ISO (ej. "2026-03-25T15:15:00.000Z") 
    a milisegundos desde la época Unix. Reemplaza la 'Z' por el offset UTC (+00:00) 
    para que Python pueda parsearlo correctamente.
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def kafka_ts_to_epoch_ms(kafka_ts_ms: int) -> str:
    """
    Kafka entrega el timestamp del mensaje en milisegundos (epoch). Timestream
    también lo quiere como texto, así que lo convertimos a str.
    """
    return str(int(kafka_ts_ms))


def build_quote_record(value: dict, kafka_ts_ms: int) -> dict:
    """
    Construye el registro para la tabla de cotizaciones (sol_quotes_raw_bigdaddyks).
    Espera el payload que genera kafka_simple_producer.py:
    {
        'symbol': 'SOLUSD',
        '@timestamp': '2026-03-09T11:21:00Z',
        'close': '123.45',
        'volume': '456.78'
    }
    """

    symbol = value.get("symbol", "UNKNOWN")
    # Usamos @timestamp si viene en el mensaje; si no, el timestamp de Kafka
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
    Construye el registro para la tabla VWAP (sol_vwap_5m_bigdaddyks).
    Espera el payload que genera SparkStreamingApp.py:
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

    # Si no viene la ventana, usamos el timestamp Kafka
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
# FUNCIÓN PRINCIPAL
# ==========================================

def main() -> None:
    # 1. Creamos el cliente de AWS para escribir en Timestream
    # Boto3 buscará nuestras credenciales de AWS automáticamente
    ts = boto3.client("timestream-write", region_name=REGION)

    # Subscribirse a los topics
    CONSUMER.subscribe([TOPIC_S5_1, TOPIC_S5_2])

    # Bucle principal: leemos y escribimos de forma continua
    while True:
        # Lee mensajes cada segundo
        records = CONSUMER.poll(timeout_ms=1000)

        if not records:
            continue

        # Procesa los mensajes
        for topic_partition, consumer_records in records.items():
            topic_name = topic_partition.topic

            for consumer_record in consumer_records:
                value = consumer_record.value
                kafka_ts_ms = consumer_record.timestamp

                try:
                    if topic_name == TOPIC_S5_1:
                        # Mensaje crudo (close/volume). Se escribe solo en sol_quotes_raw_bigdaddyks
                        quote_record = build_quote_record(value, kafka_ts_ms)

                        quote_resp = ts.write_records(
                            DatabaseName=DATABASE,
                            TableName=QUOTES_TABLE,
                            Records=[quote_record],
                        )

                        print("Write OK -> quotes", json.dumps(quote_record))

                    elif topic_name == TOPIC_S5_2:
                        # Mensaje agregado (VWAP 5m). Se escribe solo en sol_vwap_5m_bigdaddyks
                        vwap_record = build_vwap_record(value, kafka_ts_ms)

                        vwap_resp = ts.write_records(
                            DatabaseName=DATABASE,
                            TableName=VWAP_TABLE,
                            Records=[vwap_record],
                        )

                        print("Write OK -> vwap", json.dumps(vwap_record))

                    else:
                        # Topic desconocido: lo ignoramos pero avisamos
                        print(f"Topic no manejado: {topic_name}")

                except Exception as exc:
                    # Captura cualquier error de escritura o parsing para que el bucle siga vivo
                    print(f"Error procesando topic {topic_name}: {exc}")

if __name__ == "__main__":
    main()

