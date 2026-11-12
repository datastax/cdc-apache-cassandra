ARG KAFKA_IMAGE_TAG
FROM apache/kafka:${KAFKA_IMAGE_TAG}
# lib/ is the unpacked lib directory of cassandra-source-connector-<version>-kafka-connect.zip:
# the connector jar plus its runtime dependencies.
COPY lib/ /opt/kafka/plugins/cassandra-source/
