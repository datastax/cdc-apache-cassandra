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
package com.datastax.oss.kafka.source;

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.Version;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaCassandraSourceConnector extends SourceConnector {

    static final String ROW_KEY_CONVERTER_CLASS_CONFIG = "row.key.converter";
    static final String ROW_VALUE_CONVERTER_CLASS_CONFIG = "row.value.converter";
    static final String INTERNAL_CONSUMER_PARTITIONS_CONFIG = "internal.consumer.partitions";

    private Map<String, String> props;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        // validates the configuration eagerly, failing fast on bad settings.
        new CassandraSourceConnectorConfig(remapConverterProps(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaCassandraSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        CassandraSourceConnectorConfig config = new CassandraSourceConnectorConfig(remapConverterProps(props));
        int partitionCount = fetchEventsTopicPartitionCount(config);
        return taskConfigs(maxTasks, partitionCount);
    }

    /**
     * Splits the events topic's Kafka partitions round-robin across up to {@code maxTasks} tasks
     * (fewer tasks than {@code maxTasks} if there are fewer partitions), and passes each task its
     * partition list via {@link #INTERNAL_CONSUMER_PARTITIONS_CONFIG}. Each task then manually
     * {@code assign()}s exactly those partitions (see {@code KafkaCassandraSourceTask#start})
     * instead of joining a consumer group with {@code subscribe()}.
     * <p>
     * Per-key ordering is preserved because a given CQL primary key's mutations always land on
     * the same events-topic partition ({@code Murmur3KafkaPartitioner}, keyed on the partition
     * key), and this method never splits one partition's events across two tasks. That mirrors
     * the ordering guarantee Pulsar gets from Key_Shared subscriptions, without needing a
     * consumer group here.
     * <p>
     * This mapping is only recomputed when Kafka Connect calls {@code taskConfigs} again (e.g.
     * the connector is reconfigured or {@code maxTasks} changes) — it does not react to the
     * events topic's partition count changing while tasks are already running.
     */
    List<Map<String, String>> taskConfigs(int maxTasks, int partitionCount) {
        List<List<Integer>> assignments = new ArrayList<>();
        for (int i = 0; i < Math.min(maxTasks, partitionCount); i++) {
            assignments.add(new ArrayList<>());
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            assignments.get(partition % assignments.size()).add(partition);
        }
        List<Map<String, String>> configs = new ArrayList<>(assignments.size());
        for (List<Integer> partitions : assignments) {
            Map<String, String> taskProps = new HashMap<>(props);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < partitions.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(partitions.get(i));
            }
            taskProps.put(INTERNAL_CONSUMER_PARTITIONS_CONFIG, sb.toString());
            configs.add(taskProps);
        }
        return configs;
    }

    private int fetchEventsTopicPartitionCount(CassandraSourceConnectorConfig config) {
        try (AdminClient admin = AdminClient.create(InternalConsumerProperties.build(config))) {
            TopicDescription description = admin.describeTopics(java.util.Collections.singleton(config.getEventsTopic()))
                    .topicNameValues()
                    .get(config.getEventsTopic())
                    .get();
            return description.partitions().size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while describing events topic " + config.getEventsTopic(), e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Cannot describe events topic " + config.getEventsTopic(), e.getCause());
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        ConfigDef def = new ConfigDef();
        for (ConfigDef.ConfigKey key : CassandraSourceConnectorConfig.GLOBAL_CONFIG_DEF.configKeys().values()) {
            if (key.name.equals(CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG)
                    || key.name.equals(CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_TYPE_CONFIG)
                    || key.name.equals(CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG)
                    || key.name.equals(CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG)) {
                continue;
            }
            def.define(key);
        }
        def.define(ROW_KEY_CONVERTER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                null,
                ConfigDef.Importance.HIGH,
                "Converter class used to read the message key. This setting is experimental for advanced user only.");
        def.define(ROW_VALUE_CONVERTER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                null,
                ConfigDef.Importance.HIGH,
                "Converter class used to write the message value to the data topic. This setting is experimental for advanced user only.");
        return def;
    }

    static Map<String, String> remapConverterProps(Map<String, String> props) {
        Map<String, String> remapped = new HashMap<>(props);
        moveIfPresent(remapped, ROW_KEY_CONVERTER_CLASS_CONFIG, CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG);
        moveIfPresent(remapped, ROW_VALUE_CONVERTER_CLASS_CONFIG, CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG);
        return remapped;
    }

    private static void moveIfPresent(Map<String, String> props, String from, String to) {
        String value = props.remove(from);
        if (value != null) {
            props.put(to, value);
        }
    }
}
