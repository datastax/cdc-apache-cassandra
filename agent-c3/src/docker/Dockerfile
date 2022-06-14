ARG CASSANDRA_VERSION
FROM cassandra:${CASSANDRA_VERSION}
ARG BUILD_VERSION

RUN sed -i 's/cdc_enabled: false/cdc_enabled: true/g' /etc/cassandra/cassandra.yaml

COPY agent-c3-${BUILD_VERSION}-all.jar /

# Add cassandra-stress config
COPY table1.yaml /
COPY table2.yaml /

# Add the prometheus exporter
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar /
RUN chmod a+rx /jmx_prometheus_javaagent-0.16.1.jar
COPY jmx_prometheus_exporter.yaml /

# Add cassandra tools in the PATH
ENV PATH="${PATH}:/opt/cassandra/tools/bin"
