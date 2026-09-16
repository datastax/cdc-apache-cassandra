ARG KAFKA_IMAGE_TAG
FROM apache/kafka:${KAFKA_IMAGE_TAG}
ARG BUILD_VERSION
COPY pulsar-cassandra-source-${BUILD_VERSION}.jar /opt/kafka/plugins/cassandra-source/
COPY bundled-dependencies/ /opt/kafka/plugins/cassandra-source/
