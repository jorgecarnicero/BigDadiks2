# -*- coding: utf-8 -*-

import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

# Configuración
BOOTSTRAP_SERVERS="51.49.235.244:9092"
USERNAME="kafka_client"
PASSWORD="88b8a35dca1a04da57dc5f3e"
TOPIC_S5_1="imat3a_SOL_BigDaddyks"
TOPIC_S5_2="imat3a_SOL_BigDaddyks_VWAP"
GROUP_ID="imat3a_SOL_BigDaddyks"

# Crea el KafkaConsumer
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

    # Asigna topic y partición
    # CONSUMER.assign([TopicPartition(TOPIC_S5, 0)])
    CONSUMER.subscribe([TOPIC_S5_1, TOPIC_S5_2])

    while True:

        # Lee los mensajes
        records = CONSUMER.poll(timeout_ms=3600.0)

        # Procesa los mensajes
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
                    print(f"Moneda:  {data.get('symbol')}")
                    print(f"Ventana: {data.get('window_start')} a {data.get('window_end')}")
                    print(f"VWAP:    {data.get('vwap')}")
                    print("-" * 40)
                    
    # Cierra el consumidor
    # CONSUMER.close()

if __name__ == "__main__":
    main()
    


    
































################





