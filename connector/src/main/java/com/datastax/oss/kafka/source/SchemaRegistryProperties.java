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

import java.util.HashMap;
import java.util.Map;

/**
 * Translates our {@code schema.registry.*} connector settings into the property names the
 * Confluent {@code KafkaAvroSerializer} actually expects, mirroring how
 * {@link InternalConsumerProperties} remaps {@code internal.consumer.*} into plain Kafka client
 * property names.
 */
class SchemaRegistryProperties {

    private SchemaRegistryProperties() {
    }

    static Map<String, Object> build(CassandraSourceConnectorConfig config) {
        Map<String, Object> props = new HashMap<>();
        props.put("schema.registry.url", config.getSchemaRegistryUrl());
        props.put("auto.register.schemas", config.getSchemaRegistryAutoRegisterSchemas());
        putIfPresent(props, "basic.auth.credentials.source", config.getSchemaRegistryBasicAuthCredentialsSource());
        putIfPresent(props, "schema.registry.basic.auth.user.info", config.getSchemaRegistryBasicAuthUserInfo());
        return props;
    }

    private static void putIfPresent(Map<String, Object> props, String key, String value) {
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
