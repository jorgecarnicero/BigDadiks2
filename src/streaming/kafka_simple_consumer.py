# -*- coding: utf-8 -*-

import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Configuración
BOOTSTRAP_SERVERS="51.49.235.244:9092"
USERNAME="kafka_client"
PASSWORD="88b8a35dca1a04da57dc5f3e"
TOPIC="imat3a_test"
GROUP_ID="imat3a_group1"

def main() -> None:

    # Crea el KafkaConsumer
    consumer = KafkaConsumer(
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

    # Asigna topic y partición
    consumer.assign([TopicPartition(TOPIC, 0)])

    # Lee los mensajes
    records = consumer.poll(timeout_ms=3600.0)

    # Procesa los mensajes
    for topic_data, consumer_records in records.items():
        print(topic_data)
        for consumer_record in consumer_records:
            print("key:       " + str(consumer_record.key))
            print("value:     " + str(consumer_record.value))
            print("offset:    " + str(consumer_record.offset))
            print("timestamp: " + str(consumer_record.timestamp))

    # Cierra el consumidor
    consumer.close()

if __name__ == "__main__":
    main()