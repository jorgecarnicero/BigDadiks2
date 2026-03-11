# Comandos Basicos Kafka (Broker EC2)

Este archivo resume los comandos minimos para operar con tu broker Kafka:

- Broker: `51.49.235.244:9092`
- Auth: `SASL_PLAINTEXT` + `PLAIN`
- Usuario: `kafka_client`

## 1) Preparar cliente Kafka local

Instala Java:

```bash
sudo yum install java-17-amazon-corretto -y
```

Instala los binarios de Kafka en local:

```bash
cd ~
curl -L -O https://downloads.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz
```

Define variables:

```bash
export KAFKA_HOME="$HOME/kafka_2.13-3.9.2"
export BOOTSTRAP="51.49.235.244:9092"
```

## 2) Config de autenticacion (obligatoria)

Crea un archivo `client.properties`:

```bash
cat > client.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka_client" password="88b8a35dca1a04da57dc5f3e";
EOF
```

## 3) Listar topics

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config client.properties \
  --list
```

## 4) Crear un topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config client.properties \
  --create \
  --topic imat3a_test \
  --partitions 1 \
  --replication-factor 1
```

## 5) Describir un topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config client.properties \
  --describe \
  --topic imat3a_test
```

## 6) Mandar un mensaje al topic

Abrir productor interactivo:

```bash
$KAFKA_HOME/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --producer.config client.properties \
  --topic imat3a_test \
  --property parse.key=true \
  --property key.separator=,
```

Escribe clave y valor separado por una coma y pulsa Enter para enviar cada mensaje.

Enviar un unico mensaje desde terminal:

```bash
echo '1,Mensaje' | \
$KAFKA_HOME/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --producer.config client.properties \
  --topic imat3a_test \
  --property parse.key=true \
  --property key.separator=,
```

## 7) Leer mensajes del topic

```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --consumer.config client.properties \
  --topic imat3a_test \
  --property print.key=true \
  --consumer-property group.id=imat3a_group1
```
## 8) Listar los consumer groups

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config client.properties \
  --list
```

## 9) Ver la posición de un consumer group en cada topic/partition

```bash
$KAFKA_HOME/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config client.properties \
  --describe \
  --group imat3a_group1
```

## 10) Borrar un topic

```bash
$KAFKA_HOME/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config client.properties \
  --delete \
  --topic imat3a_test
```