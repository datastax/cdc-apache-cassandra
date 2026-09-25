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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the Cassandra Kafka Connect sink connector.
 *
 * <p>Cassandra connection, cache and output-format settings are delegated to the existing
 * {@link CassandraSourceConnectorConfig} (the same keys as the Pulsar connector: {@code keyspace},
 * {@code table}, {@code contactPoints}, {@code loadBalancing.localDc}, {@code cache.*},
 * {@code outputFormat}, SSL/auth, etc.). This class adds the Kafka-specific settings needed to
 * publish the resulting rows to the data topic.
 */
public class CassandraSinkConfig {

    /** Bootstrap servers for the producer that writes enriched rows to the data topic. */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

    /** Prefix for the data topic; the data topic is {@code <prefix><keyspace>.<table>}. */
    public static final String DATA_TOPIC_PREFIX = "data.topic.prefix";
    public static final String DATA_TOPIC_PREFIX_DEFAULT = "data-";

    /**
     * Optional Confluent Schema Registry URL. When unset (default) the connector reads the events
     * topic and writes the data topic using registry-less raw AVRO, matching the agent default.
     */
    public static final String SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";

    private final Map<String, String> originals;
    private final CassandraSourceConnectorConfig cassandraConfig;
    private final String bootstrapServers;
    private final String dataTopicPrefix;
    private final String schemaRegistryUrl;

    public CassandraSinkConfig(Map<String, String> props) {
        this.originals = props;
        // The reused CassandraSourceConnectorConfig requires the Pulsar-specific 'events.topic'.
        // For Kafka, events are delivered by the Connect framework (the input 'topics'); derive a
        // sensible default of events-<keyspace>.<table> when not explicitly provided.
        Map<String, String> effective = new HashMap<>(props);
        if (!effective.containsKey("events.topic")) {
            String keyspace = props.get("keyspace");
            String table = props.get("table");
            if (keyspace != null && table != null) {
                effective.put("events.topic", "events-" + keyspace + "." + table);
            }
        }
        this.cassandraConfig = new CassandraSourceConnectorConfig(effective);
        this.bootstrapServers = props.get(KAFKA_BOOTSTRAP_SERVERS);
        this.dataTopicPrefix = props.getOrDefault(DATA_TOPIC_PREFIX, DATA_TOPIC_PREFIX_DEFAULT);
        String registry = props.get(SCHEMA_REGISTRY_URL);
        this.schemaRegistryUrl = (registry == null || registry.trim().isEmpty()) ? null : registry.trim();
    }

    public CassandraSourceConnectorConfig getCassandraConfig() {
        return cassandraConfig;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getKeyspace() {
        return cassandraConfig.getKeyspaceName();
    }

    public String getTable() {
        return cassandraConfig.getTableName();
    }

    /** Data topic name: {@code <prefix><keyspace>.<table>}. */
    public String getDataTopic() {
        return dataTopicPrefix + getKeyspace() + "." + getTable();
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public Map<String, String> originals() {
        return originals;
    }

    /**
     * Minimal {@link ConfigDef} for Kafka Connect validation/UI. The remaining Cassandra settings
     * are read directly from the raw configuration by {@link CassandraSourceConnectorConfig}.
     */
    public static ConfigDef configDef() {
        return new ConfigDef()
                .define("keyspace", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Cassandra keyspace to query.")
                .define("table", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Cassandra table to query.")
                .define("contactPoints", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Comma-separated Cassandra contact points.")
                .define("loadBalancing.localDc", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Cassandra local datacenter.")
                .define(KAFKA_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Bootstrap servers for the data-topic producer.")
                .define(DATA_TOPIC_PREFIX, ConfigDef.Type.STRING, DATA_TOPIC_PREFIX_DEFAULT,
                        ConfigDef.Importance.MEDIUM, "Prefix for the output data topic.")
                .define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Optional Confluent Schema Registry URL (registry-less raw AVRO if unset).");
    }
}
