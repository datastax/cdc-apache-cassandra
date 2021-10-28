ARG CASSANDRA_VERSION
FROM cassandra:${CASSANDRA_VERSION}
ARG BUILD_VERSION

# Add utilities to run the CDC test from a DSE pod
RUN set -eux; apt-get update; apt-get install -y --no-install-recommends jq curl wget; rm -rf /var/lib/apt/lists/*;
ADD https://github.com/josephburnett/jd/releases/download/v1.4.0/jd-amd64-linux /bin/
RUN chmod a+x /bin/jd-amd64-linux && mv /bin/jd-amd64-linux /bin/jd
RUN cd / && wget 'https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-2.8.0/apache-pulsar-2.8.0-bin.tar.gz' --no-verbose -O pulsar.tar.gz \
    && tar zxvf pulsar.tar.gz \
    && mkdir /apache-pulsar-2.8.0/logs \
    && chmod a+rwx /apache-pulsar-2.8.0/logs/

RUN sed -i 's/cdc_enabled: false/cdc_enabled: true/g' /etc/cassandra/cassandra.yaml
COPY producer-v4-pulsar-${BUILD_VERSION}-all.jar /
COPY table1.yaml /

# Add cassandra tools in the PATH
ENV PATH="${PATH}:/opt/cassandra/tools/bin"
