ARG PULSAR_VERSION
FROM datastax/lunastreaming:${PULSAR_VERSION}
ARG BUILD_VERSION
COPY pulsar-cassandra-source-${BUILD_VERSION}.nar /pulsar/connectors/
