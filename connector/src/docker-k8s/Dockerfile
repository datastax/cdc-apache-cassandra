ARG KAFKA_IMAGE_TAG
FROM apache/kafka:${KAFKA_IMAGE_TAG}
# lib/ is the unpacked lib directory of cassandra-source-connector-<version>-kafka-connect.zip:
# the connector jar plus its runtime dependencies.
COPY lib/ /opt/kafka/plugins/cassandra-source/
# Aiven OpenSearch sink and Confluent Avro converter, so the sink can read the Avro data topics.
COPY opensearch-sink/ /opt/kafka/plugins/opensearch-sink/
COPY avro-converter/ /opt/kafka/plugins/avro-converter/
