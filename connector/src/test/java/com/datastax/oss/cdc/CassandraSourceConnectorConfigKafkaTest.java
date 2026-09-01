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
package com.datastax.oss.cdc;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.datastax.oss.cdc.CassandraSourceConnectorConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

class CassandraSourceConnectorConfigKafkaTest {

    @Test
    void should_default_internal_consumer_settings() {
        CassandraSourceConnectorConfig config = new CassandraSourceConnectorConfig(requiredSettings());

        assertThat(config.getOutputTopic()).isEqualTo("");
        assertThat(config.getHeartbeatTopic()).isEqualTo("");
        assertThat(config.getInternalConsumerBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.getInternalConsumerGroupId()).isEqualTo("cassandra-source");
        assertThat(config.getInternalConsumerSecurityProtocol()).isEqualTo("");
        assertThat(config.getInternalConsumerSslKeystoreLocation()).isEqualTo("");
        assertThat(config.getInternalConsumerSslKeystorePassword()).isEqualTo("");
        assertThat(config.getInternalConsumerSslTruststoreLocation()).isEqualTo("");
        assertThat(config.getInternalConsumerSslTruststorePassword()).isEqualTo("");
        assertThat(config.getInternalConsumerSaslMechanism()).isEqualTo("");
        assertThat(config.getInternalConsumerSaslJaasConfig()).isEqualTo("");
    }

    @Test
    void should_override_internal_consumer_settings() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .putAll(requiredSettings())
                        .put(OUTPUT_TOPIC_CONFIG, "data-ks1.table1")
                        .put(HEARTBEAT_TOPIC_CONFIG, "data-ks1.table1-hb")
                        .put(INTERNAL_CONSUMER_BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092")
                        .put(INTERNAL_CONSUMER_GROUP_ID_CONFIG, "my-connector")
                        .put(INTERNAL_CONSUMER_SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                        .put(INTERNAL_CONSUMER_SSL_KEYSTORE_LOCATION_CONFIG, "/keystore.jks")
                        .put(INTERNAL_CONSUMER_SSL_KEYSTORE_PASSWORD_CONFIG, "keystorepass")
                        .put(INTERNAL_CONSUMER_SSL_TRUSTSTORE_LOCATION_CONFIG, "/truststore.jks")
                        .put(INTERNAL_CONSUMER_SSL_TRUSTSTORE_PASSWORD_CONFIG, "truststorepass")
                        .put(INTERNAL_CONSUMER_SASL_MECHANISM_CONFIG, "PLAIN")
                        .put(INTERNAL_CONSUMER_SASL_JAAS_CONFIG_CONFIG, "jaas-config-value")
                        .build();

        CassandraSourceConnectorConfig config = new CassandraSourceConnectorConfig(props);

        assertThat(config.getOutputTopic()).isEqualTo("data-ks1.table1");
        assertThat(config.getHeartbeatTopic()).isEqualTo("data-ks1.table1-hb");
        assertThat(config.getInternalConsumerBootstrapServers()).isEqualTo("kafka1:9092,kafka2:9092");
        assertThat(config.getInternalConsumerGroupId()).isEqualTo("my-connector");
        assertThat(config.getInternalConsumerSecurityProtocol()).isEqualTo("SASL_SSL");
        assertThat(config.getInternalConsumerSslKeystoreLocation()).isEqualTo("/keystore.jks");
        assertThat(config.getInternalConsumerSslKeystorePassword()).isEqualTo("keystorepass");
        assertThat(config.getInternalConsumerSslTruststoreLocation()).isEqualTo("/truststore.jks");
        assertThat(config.getInternalConsumerSslTruststorePassword()).isEqualTo("truststorepass");
        assertThat(config.getInternalConsumerSaslMechanism()).isEqualTo("PLAIN");
        assertThat(config.getInternalConsumerSaslJaasConfig()).isEqualTo("jaas-config-value");
    }

    @Test
    void should_leave_pulsar_only_settings_untouched() {
        CassandraSourceConnectorConfig config = new CassandraSourceConnectorConfig(requiredSettings());

        assertThat(config.getEventsSubscriptionName()).isEqualTo("sub");
        assertThat(config.getEventsSubscriptionType()).isEqualTo("Key_Shared");
    }

    Map<String, String> requiredSettings() {
        return ImmutableMap.<String, String>builder()
                .put(KEYSPACE_NAME_CONFIG, "ks1")
                .put(TABLE_NAME_CONFIG, "table1")
                .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                .build();
    }
}
