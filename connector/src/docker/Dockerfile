ARG LUNASTREAMING_VERSION
FROM datastax/lunastreaming:${LUNASTREAMING_VERSION}
ARG BUILD_VERSION
COPY pulsar-cassandra-source-${BUILD_VERSION}.nar /pulsar/connectors/
COPY pulsar-io-elastic-search-2.7.2.1.1.6.nar /pulsar/connectors/
