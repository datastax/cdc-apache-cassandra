ARG DSE_VERSION
FROM datastax/dse-server:${DSE_VERSION}
ARG BUILD_VERSION
ARG COMMITMOG_SYNC_PERIOD_IN_MS

RUN sed -i 's/cdc_enabled: false/cdc_enabled: true/g' /opt/dse/resources/cassandra/conf/cassandra.yaml
RUN sed -i "s/commitlog_sync_period_in_ms: 10000/commitlog_sync_period_in_ms: $COMMITMOG_SYNC_PERIOD_IN_MS/g" /opt/dse/resources/cassandra/conf/cassandra.yaml

COPY producer-dse4-pulsar-${BUILD_VERSION}-all.jar /
COPY table1.yaml /

# Add cassandra tools in the PATH
ENV PATH="${PATH}:/opt/dse/resources/cassandra/tools/bin/"
