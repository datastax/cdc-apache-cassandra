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

import com.datastax.oss.cdc.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka Connect sink connector that mirrors the Pulsar {@code CassandraSource}: it consumes CDC
 * mutation events from the {@code events-<keyspace>.<table>} topic (produced by the CDC agent),
 * queries Cassandra for the current row, and publishes the row to the {@code data-<keyspace>.<table>}
 * topic.
 *
 * <p>Deploy with {@code key.converter} / {@code value.converter} set to
 * {@code org.apache.kafka.connect.converters.ByteArrayConverter} so the task receives the raw AVRO
 * bytes produced by the agent.
 */
public class CassandraSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(CassandraSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        this.configProps = new HashMap<>(props);
        // Fail fast on invalid configuration.
        new CassandraSinkConfig(this.configProps);
        log.info("Starting CassandraSinkConnector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CassandraSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(configProps));
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping CassandraSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return CassandraSinkConfig.configDef();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
