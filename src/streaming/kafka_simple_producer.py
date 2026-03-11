# -*- coding: utf-8 -*-

import json
from kafka import KafkaProducer

# Configuración
BOOTSTRAP_SERVERS="51.49.235.244:9092"
USERNAME="kafka_client"
PASSWORD="88b8a35dca1a04da57dc5f3e"
TOPIC="imat3a_test"

def main() -> None:

    # Crea el KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8")
    )

    # Crea el mensaje
    key = "key1"
    value = {
        "field1": "value1"
    }

    # Muestra el mensaje a enviar
    print("Mensaje a enviar: ", key + " " + str(value))

    # Envía el mensaje
    producer.send(topic=TOPIC, key=key, value=value)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()