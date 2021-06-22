/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.Serializable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration class for the Cassandra CDC Source Connector.
 */
@Data
@Accessors(chain = true)
public class CassandraSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Cassandra keyspace name is a mandatory field"
    )
    private String keyspace;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Cassandra keyspace name is a mandatory field"
    )
    private String table;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Regular expression of the Cassandra replicated column names"
    )
    private String columns;

    @FieldDoc(
        required = true,
        defaultValue = "9042",
        help = "Port to connect to nodes"
    )
    private int port = 9042;

    @FieldDoc(
        required = true,
        defaultValue = "500",
        help = "The maximum number of requests to send at once"
    )
    private int maxConcurrentRequests = 500;

    @FieldDoc(
        required = false,
        defaultValue = "false",
        help = "Whether to enable JMX reporting"
    )
    private boolean jmx = false;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Domain for JMX reporting"
    )
    private String jmxConnectorDomain;

    @FieldDoc(
        required = false,
        defaultValue = "None",
        help = "None | LZ4 | Snappy"
    )
    private String compression = "None";

    @FieldDoc(
        required = true,
        defaultValue = "30",
        help = "CQL statement execution timeout, in seconds"
    )
    private int queryExecutionTimeout = 30;

    @FieldDoc(
        required = true,
        defaultValue = "35",
        help = "This is used to scale internal data structures for gathering metrics. "
                + "It should be higher than queryExecutionTimeout. This parameter should be expressed in seconds."
    )
    private int metricsHighestLatency = 35;

    @FieldDoc(
        required = true,
        defaultValue = "32",
        help = "Maximum number of records that could be send in one batch request"
    )
    private int maxNumberOfRecordsInBatch = 32;

    @FieldDoc(
        required = true,
        defaultValue = "4",
        help = "Number of connections that driver maintains within a connection pool to each node in local dc"
    )
    private int connectionPoolLocalSize = 4;

    @FieldDoc(
        required = true,
        defaultValue = "None",
        help = "Specifies which errors the connector should ignore when processing the record. "
                + "Valid values are: "
                + "None (never ignore errors), "
                + "All (ignore all errors), "
                + "Driver (ignore driver errors only, i.e. errors when writing to the database)." 
    )
    private String ignoreErrors = "None";

    public static CassandraSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), CassandraSourceConfig.class);
    }

    public static CassandraSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), CassandraSourceConfig.class);
    }

    public void validate() {
        if (StringUtils.isEmpty(keyspace)) {
            throw new IllegalArgumentException("keyspace not set.");
        }

        if (StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("table not set.");
        }

        if (StringUtils.isEmpty(columns)) {
            throw new IllegalArgumentException("columns not set.");
        }

        if (maxConcurrentRequests <= 0) {
            throw new IllegalArgumentException("maxConcurrentRequests must be a positive integer.");
        }

        if (queryExecutionTimeout <= 0) {
            throw new IllegalArgumentException("queryExecutionTimeout must be a positive integer.");
        }

        if (metricsHighestLatency <= 0) {
            throw new IllegalArgumentException("metricsHightestLatency must be a positive integer.");
        }

        if (metricsHighestLatency <= queryExecutionTimeout) {
            throw new IllegalArgumentException("metricsHighestLatency must be larger than metricsHighestLatency.");
        }

        if (maxNumberOfRecordsInBatch <= 0) {
            throw new IllegalArgumentException("maxNumberOfRecordsInBatch must be a positive integer.");
        }

        if (connectionPoolLocalSize <= 0) {
            throw new IllegalArgumentException("connectionPoolLocalSize must be a positive integer.");
        }

    }
}
