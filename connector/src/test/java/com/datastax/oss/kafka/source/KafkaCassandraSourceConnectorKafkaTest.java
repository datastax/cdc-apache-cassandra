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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaCassandraSourceConnectorKafkaTest {

    @Test
    void should_split_partitions_evenly_across_tasks() {
        KafkaCassandraSourceConnector connector = new KafkaCassandraSourceConnector();
        connector.start(requiredProps());

        List<Map<String, String>> configs = connector.taskConfigs(3, 6);

        assertThat(configs).hasSize(3);
        assertThat(configs.get(0).get(KafkaCassandraSourceConnector.INTERNAL_CONSUMER_PARTITIONS_CONFIG)).isEqualTo("0,3");
        assertThat(configs.get(1).get(KafkaCassandraSourceConnector.INTERNAL_CONSUMER_PARTITIONS_CONFIG)).isEqualTo("1,4");
        assertThat(configs.get(2).get(KafkaCassandraSourceConnector.INTERNAL_CONSUMER_PARTITIONS_CONFIG)).isEqualTo("2,5");
    }

    @Test
    void should_cap_task_count_at_partition_count() {
        KafkaCassandraSourceConnector connector = new KafkaCassandraSourceConnector();
        connector.start(requiredProps());

        List<Map<String, String>> configs = connector.taskConfigs(10, 2);

        assertThat(configs).hasSize(2);
    }

    @Test
    void should_carry_connector_props_into_each_task_config() {
        KafkaCassandraSourceConnector connector = new KafkaCassandraSourceConnector();
        connector.start(requiredProps());

        List<Map<String, String>> configs = connector.taskConfigs(2, 2);

        for (Map<String, String> taskProps : configs) {
            assertThat(taskProps.get(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG)).isEqualTo("ks1");
        }
    }

    @Test
    void should_expose_row_key_and_value_converter_settings_instead_of_key_value_converter() {
        KafkaCassandraSourceConnector connector = new KafkaCassandraSourceConnector();

        ConfigDef def = connector.config();

        assertThat(def.configKeys()).containsKeys(
                KafkaCassandraSourceConnector.ROW_KEY_CONVERTER_CLASS_CONFIG,
                KafkaCassandraSourceConnector.ROW_VALUE_CONVERTER_CLASS_CONFIG);
        assertThat(def.configKeys()).doesNotContainKeys(
                CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
                CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG,
                CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_NAME_CONFIG,
                CassandraSourceConnectorConfig.EVENTS_SUBSCRIPTION_TYPE_CONFIG);
    }

    private Map<String, String> requiredProps() {
        return ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "table1")
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, "data-ks1.table1")
                .build();
    }
}
