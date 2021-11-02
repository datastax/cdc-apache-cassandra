ARG CASSANDRA_VERSION
FROM cassandra:${CASSANDRA_VERSION}
ARG BUILD_VERSION

RUN sed -i 's/cdc_enabled: false/cdc_enabled: true/g' /etc/cassandra/cassandra.yaml

COPY producer-v3-pulsar-${BUILD_VERSION}-all.jar /
COPY table1.yaml /
COPY table2.yaml /

# Add cassandra tools in the PATH
ENV PATH="${PATH}:/opt/cassandra/tools/bin"
