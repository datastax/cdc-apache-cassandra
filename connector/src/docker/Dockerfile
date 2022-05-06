ARG LUNASTREAMING_IMAGE
ARG LUNASTREAMING_VERSION
FROM datastax/lunastreaming-all:2.8.0_1.1.40 as lunastreaming-all

FROM ${LUNASTREAMING_IMAGE}:${LUNASTREAMING_VERSION}
ARG BUILD_VERSION
COPY pulsar-cassandra-source-${BUILD_VERSION}.nar /pulsar/connectors/
COPY --from=lunastreaming-all /pulsar/connectors/pulsar-io-elastic-search-2.8.0.1.1.40.nar /pulsar/connectors

