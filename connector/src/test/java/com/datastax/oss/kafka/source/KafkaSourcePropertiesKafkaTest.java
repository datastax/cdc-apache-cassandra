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
import com.datastax.oss.common.sink.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the property translation helpers used by the Kafka source connector and task.
 */
class KafkaSourcePropertiesKafkaTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1");
        props.put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, "t1");
        props.put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, "events-ks1.t1");
        props.put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, "data-ks1.t1");
        props.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        return props;
    }

    @Test
    void internalConsumerPropertiesOnlyBootstrapByDefault() {
        Properties props = InternalConsumerProperties.build(new CassandraSourceConnectorConfig(baseProps()));
        assertEquals("broker:9092", props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertFalse(props.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertFalse(props.containsKey(SaslConfigs.SASL_JAAS_CONFIG));
        assertFalse(props.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    }

    @Test
    void internalConsumerPropertiesMapsSecuritySettings() {
        Map<String, String> settings = baseProps();
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SSL_KEYSTORE_LOCATION_CONFIG, "/ks.jks");
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SSL_KEYSTORE_PASSWORD_CONFIG, "kspass");
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SSL_TRUSTSTORE_LOCATION_CONFIG, "/ts.jks");
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SSL_TRUSTSTORE_PASSWORD_CONFIG, "tspass");
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SASL_MECHANISM_CONFIG, "PLAIN");
        settings.put(CassandraSourceConnectorConfig.INTERNAL_CONSUMER_SASL_JAAS_CONFIG_CONFIG, "jaas");

        Properties props = InternalConsumerProperties.build(new CassandraSourceConnectorConfig(settings));

        assertEquals("SASL_SSL", props.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("/ks.jks", props.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("kspass", props.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertEquals("/ts.jks", props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("tspass", props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals("PLAIN", props.get(SaslConfigs.SASL_MECHANISM));
        assertEquals("jaas", props.get(SaslConfigs.SASL_JAAS_CONFIG));
    }

    @Test
    void schemaRegistryPropertiesWithoutBasicAuth() {
        Map<String, String> settings = baseProps();
        settings.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://registry:8081");

        Map<String, Object> props = SchemaRegistryProperties.build(new CassandraSourceConnectorConfig(settings));

        assertEquals("http://registry:8081", props.get("schema.registry.url"));
        assertNotNull(props.get("auto.register.schemas"));
        assertFalse(props.containsKey("basic.auth.credentials.source"));
        assertFalse(props.containsKey("schema.registry.basic.auth.user.info"));
    }

    @Test
    void schemaRegistryPropertiesWithBasicAuth() {
        Map<String, String> settings = baseProps();
        settings.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://registry:8081");
        settings.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS_CONFIG, "false");
        settings.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG, "USER_INFO");
        settings.put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO_CONFIG, "user:secret");

        Map<String, Object> props = SchemaRegistryProperties.build(new CassandraSourceConnectorConfig(settings));

        assertEquals(false, props.get("auto.register.schemas"));
        assertEquals("USER_INFO", props.get("basic.auth.credentials.source"));
        assertEquals("user:secret", props.get("schema.registry.basic.auth.user.info"));
    }

    @Test
    void connectorReportsVersionAndTaskClass() {
        KafkaCassandraSourceConnector connector = new KafkaCassandraSourceConnector();
        assertNotNull(connector.version());
        assertEquals(KafkaCassandraSourceTask.class, connector.taskClass());
    }

    @Test
    void connectorStartValidatesConfig() {
        KafkaCassandraSourceConnector connector = new KafkaCassandraSourceConnector();
        connector.start(baseProps());
        connector.stop();

        Map<String, String> missingKeyspace = baseProps();
        missingKeyspace.remove(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG);
        assertThrows(ConfigException.class, () -> connector.start(missingKeyspace));
    }

    @Test
    void remapsRowConverterSettings() {
        Map<String, String> props = baseProps();
        props.put(KafkaCassandraSourceConnector.ROW_KEY_CONVERTER_CLASS_CONFIG, "key.Converter");
        props.put(KafkaCassandraSourceConnector.ROW_VALUE_CONVERTER_CLASS_CONFIG, "value.Converter");

        Map<String, String> remapped = KafkaCassandraSourceConnector.remapConverterProps(props);

        assertEquals("key.Converter", remapped.get(CassandraSourceConnectorConfig.KEY_CONVERTER_CLASS_CONFIG));
        assertEquals("value.Converter", remapped.get(CassandraSourceConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG));
        assertFalse(remapped.containsKey(KafkaCassandraSourceConnector.ROW_KEY_CONVERTER_CLASS_CONFIG));
        assertEquals("ks1", remapped.get(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG));
    }
}
