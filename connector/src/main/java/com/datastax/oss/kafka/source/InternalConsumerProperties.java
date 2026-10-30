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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * Security properties shared by the internal AdminClient (used to size tasks) and the internal
 * KafkaConsumer each task runs against the events topic. Mirrors the property names the CDC
 * agent's Kafka sender already uses, prefixed with internal.consumer.
 */
class InternalConsumerProperties {

    private InternalConsumerProperties() {
    }

    static Properties build(CassandraSourceConnectorConfig config) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getInternalConsumerBootstrapServers());
        putIfPresent(props, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.getInternalConsumerSecurityProtocol());
        putIfPresent(props, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.getInternalConsumerSslKeystoreLocation());
        putIfPresent(props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.getInternalConsumerSslKeystorePassword());
        putIfPresent(props, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.getInternalConsumerSslTruststoreLocation());
        putIfPresent(props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.getInternalConsumerSslTruststorePassword());
        putIfPresent(props, SaslConfigs.SASL_MECHANISM, config.getInternalConsumerSaslMechanism());
        putIfPresent(props, SaslConfigs.SASL_JAAS_CONFIG, config.getInternalConsumerSaslJaasConfig());
        return props;
    }

    private static void putIfPresent(Properties props, String key, String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
