ARG DSE_VERSION
FROM datastax/dse-server:${DSE_VERSION}
ARG BUILD_VERSION
ARG COMMITMOG_SYNC_PERIOD_IN_MS
ARG CDC_TOTAL_SPACE_IN_MB

RUN sed -i 's/cdc_enabled: false/cdc_enabled: true/g' /opt/dse/resources/cassandra/conf/cassandra.yaml
RUN sed -i "s/commitlog_sync_period_in_ms: 10000/commitlog_sync_period_in_ms: $COMMITMOG_SYNC_PERIOD_IN_MS/g" /opt/dse/resources/cassandra/conf/cassandra.yaml
RUN echo "cdc_total_space_in_mb: ${CDC_TOTAL_SPACE_IN_MB}" >> /opt/dse/resources/cassandra/conf/cassandra.yaml

COPY agent-dse4-${BUILD_VERSION}-all.jar /

# Add cassandra-stress config
COPY table1.yaml /

# Add the prometheus exporter
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar /
USER root
RUN chmod a+rx /jmx_prometheus_javaagent-0.16.1.jar
USER dse
COPY jmx_prometheus_exporter.yaml /


# Add cassandra tools in the PATH
ENV PATH="${PATH}:/opt/dse/resources/cassandra/tools/bin/"
