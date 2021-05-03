/**
 * Copyright DataStax, Inc 2021.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.cdc.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class ProducerConfig {

    public static final String storageDir = System.getProperty("cassandra.storagedir", null);

    @AllArgsConstructor
    public static class Setting<T> {
        public final String name;
        public final Function<String, T> initializer;
        public final Supplier<T> supplier;
    }

    public static final String CDC_RELOCATION_DIR = "cdcRelocationDir";
    public static String cdcRelocationDir = System.getProperty(CDC_RELOCATION_DIR, storageDir + File.separator + "cdc_backup");
    public static final Setting<String> CDC_RELOCATION_DIR_SETTING = new Setting<>(CDC_RELOCATION_DIR, s -> cdcRelocationDir = s, () -> cdcRelocationDir);

    public static final String CDC_DIR_POOL_INTERVAL_MS = "cdcPoolIntervalMs";
    public static long cdcDirPollIntervalMs = Long.getLong(CDC_DIR_POOL_INTERVAL_MS, 60000L);
    public static final Setting<Long> CDC_DIR_POOL_INTERVAL_MS_SETTING =
            new Setting<>(CDC_DIR_POOL_INTERVAL_MS, s -> cdcDirPollIntervalMs = Long.parseLong(s), () -> cdcDirPollIntervalMs);

    public static final String ERROR_COMMITLOG_REPROCESS_ENABLED = "errorCommitLogReprocessEnabled";
    public static boolean errorCommitLogReprocessEnabled = Boolean.getBoolean(ERROR_COMMITLOG_REPROCESS_ENABLED);
    public static final Setting<Boolean> ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING =
            new Setting<>(ERROR_COMMITLOG_REPROCESS_ENABLED, s -> errorCommitLogReprocessEnabled = Boolean.parseBoolean(s), () -> errorCommitLogReprocessEnabled);

    public static final String EMIT_TOMBSTONE_ON_DELETE = "emitTombstoneOnDelete";
    public static boolean emitTombstoneOnDelete = Boolean.getBoolean(EMIT_TOMBSTONE_ON_DELETE);
    public static final Setting<Boolean> EMIT_TOMBSTONE_ON_DELETE_SETTING =
            new Setting<>(EMIT_TOMBSTONE_ON_DELETE, s -> emitTombstoneOnDelete = Boolean.parseBoolean(s), () -> emitTombstoneOnDelete);

    public static final String TOPIC_PREFIX = "topicPrefix";
    public static String topicPrefix = System.getProperty(TOPIC_PREFIX, "events-");
    public static final Setting<String> TOPIC_PREFIX_SETTING =
            new Setting<>(TOPIC_PREFIX, s -> topicPrefix = s, () -> topicPrefix);

    public static final String PULSAR_SERVICE_URL = "pulsarServiceUrl";
    public static String pulsarServiceUrl = System.getProperty(PULSAR_SERVICE_URL, "pulsar://localhost:6650");
    public static final Setting<String> PULSAR_SERVICE_URL_SETTING =
            new Setting<>(PULSAR_SERVICE_URL, s -> pulsarServiceUrl = s, () -> pulsarServiceUrl);

    public static final String KAFKA_BROKERS = "kafkaBrokers";
    public static String kafkaBrokers = System.getProperty(KAFKA_BROKERS, "localhost:9092");
    public static final Setting<String> KAFKA_BROKERS_SETTING =
            new Setting<>(KAFKA_BROKERS, s -> kafkaBrokers = s, () -> kafkaBrokers);

    public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafkaSchemaRegistryUrl";
    public static String kafkaSchemaRegistryUrl = System.getProperty(KAFKA_SCHEMA_REGISTRY_URL, "http://localhost:8081");
    public static final Setting<String> KAFKA_SCHEMA_REGISTRY_URL_SETTING =
            new Setting<>(KAFKA_SCHEMA_REGISTRY_URL, s -> kafkaSchemaRegistryUrl = s, () -> kafkaSchemaRegistryUrl);

    public static final String SSL_PROVIDER = "sslProvider";
    public static String sslProvider = System.getProperty(SSL_PROVIDER);
    public static final Setting<String> SSL_PROVIDER_SETTING =
            new Setting<>(SSL_PROVIDER, s -> sslProvider = s, () -> sslProvider);

    public static final String SSL_TRUSTSTORE_PATH = "sslTruststorePath";
    public static String sslTruststorePath = System.getProperty(SSL_TRUSTSTORE_PATH);
    public static final Setting<String> SSL_TRUSTSTORE_PATH_SETTING =
            new Setting<>(SSL_TRUSTSTORE_PATH, s -> sslTruststorePath = s, () -> sslTruststorePath);

    public static final String SSL_TRUSTSTORE_PASSWORD = "sslTruststorePassword";
    public static String sslTruststorePassword = System.getProperty(SSL_TRUSTSTORE_PASSWORD);
    public static final Setting<String> SSL_TRUSTSTORE_PASSWORD_SETTING =
            new Setting<>(SSL_TRUSTSTORE_PASSWORD, s -> sslTruststorePassword = s, () -> sslTruststorePassword);

    public static final String SSL_TRUSTSTORE_TYPE = "sslTruststoreType";
    public static String sslTruststoreType = System.getProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    public static final Setting<String> SSL_TRUSTSTORE_TYPE_SETTING =
            new Setting<>(SSL_TRUSTSTORE_TYPE, s -> sslTruststoreType = s, () -> sslTruststoreType);

    public static final String SSL_KEYSTORE_PATH = "sslKeystorePath";
    public static String sslKeystorePath = System.getProperty(SSL_KEYSTORE_PATH);
    public static final Setting<String> SSL_KEYSTORE_PATH_SETTING =
            new Setting<>(SSL_KEYSTORE_PATH, s -> sslKeystorePath = s, () -> sslKeystorePath);

    public static final String SSL_KEYSTORE_PASSWORD = "sslKeystorePassword";
    public static String sslKeystorePassword = System.getProperty(SSL_KEYSTORE_PASSWORD);
    public static final Setting<String> SSL_KEYSTORE_PASSWORD_SETTING =
            new Setting<>(SSL_KEYSTORE_PASSWORD, s -> sslKeystorePassword = s, () -> sslKeystorePassword);

    public static final String SSL_CIPHER_SUITES = "sslCipherSuites";
    public static String sslCipherSuites = System.getProperty(SSL_CIPHER_SUITES);
    public static final Setting<String> SSL_CIPHER_SUITES_SETTING =
            new Setting<>(SSL_CIPHER_SUITES, s -> sslCipherSuites = s, () -> sslCipherSuites);

    public static final String SSL_ENABLED_PROTOCOLS = "sslEnabledProtocols";
    public static String sslEnabledProtocols = System.getProperty(SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1,TLSv1");
    public static final Setting<String> SSL_ENABLED_PROTOCOLS_SETTING =
            new Setting<>(SSL_ENABLED_PROTOCOLS, s -> sslEnabledProtocols = s, () -> sslEnabledProtocols);

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "sslEndpointIdentificationAlgorithm";
    public static String sslEndpointIdentificationAlgorithm = System.getProperty(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
    public static final Setting<String> SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_SETTING =
            new Setting<>(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, s -> sslEndpointIdentificationAlgorithm = s, () -> sslEndpointIdentificationAlgorithm);

    public static final String SSL_ALLOW_INSECURE_CONNECTION = "sslAllowInsecureConnection";
    public static boolean sslAllowInsecureConnection = Boolean.getBoolean(SSL_ALLOW_INSECURE_CONNECTION);
    public static final Setting<Boolean> SSL_ALLOW_INSECURE_CONNECTION_SETTING =
            new Setting<>(SSL_ALLOW_INSECURE_CONNECTION, s -> sslAllowInsecureConnection = Boolean.parseBoolean(s), () -> sslAllowInsecureConnection);

    public static final String SSL_HOSTNAME_VERIFICATION_ENABLE = "sslHostnameVerificationEnable";
    public static boolean sslHostnameVerificationEnable = Boolean.getBoolean(SSL_HOSTNAME_VERIFICATION_ENABLE);
    public static final Setting<Boolean> SSL_HOSTNAME_VERIFICATION_ENABLE_SETTING =
            new Setting<>(SSL_HOSTNAME_VERIFICATION_ENABLE, s -> sslHostnameVerificationEnable = Boolean.parseBoolean(s), () -> sslHostnameVerificationEnable);

    public static final String PULSAR_AUTH_PLUGIN_CLASS_NAME = "pulsarAuthPluginClassName";
    public static String pulsarAuthPluginClassName = System.getProperty(PULSAR_AUTH_PLUGIN_CLASS_NAME);
    public static final Setting<String> PULSAR_AUTH_PLUGIN_CLASS_NAME_SETTING =
            new Setting<>(PULSAR_AUTH_PLUGIN_CLASS_NAME, s -> pulsarAuthPluginClassName = s, () -> pulsarAuthPluginClassName);

    public static final String PULSAR_AUTH_PARAMS = "pulsarAuthParams";
    public static String pulsarAuthParams = System.getProperty(PULSAR_AUTH_PARAMS);
    public static final Setting<String> PULSAR_AUTH_PARAMS_SETTING =
            new Setting<>(PULSAR_AUTH_PARAMS, s -> pulsarAuthParams = s, () -> pulsarAuthParams);

    // generic properties for kafka client
    public static final String KAFKA_PROPERTIES = "kafkaProperties";
    public static Map<String, String> kafkaProperties = new HashMap<>();
    public static final Setting<Map<String, String>> KAFKA_PROPERTIES_SETTINGS =
            new Setting<>(KAFKA_PROPERTIES,
                    s -> {
                        for (String param : s.split(",")) {
                            int i = param.indexOf("=");
                            if (i > 0) {
                                kafkaProperties.put(param.substring(0, i), param.substring(i + 1));
                            }
                        }
                        return kafkaProperties;
                    },
                    () -> kafkaProperties);

    public static final Set<Setting<?>> settings;
    public static final Map<String, Setting<?>> settingMap;

    static {
        // don't use guava
        Set<Setting<?>> set = new HashSet<>();
        set.add(CDC_RELOCATION_DIR_SETTING);
        set.add(CDC_DIR_POOL_INTERVAL_MS_SETTING);
        set.add(ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING);
        set.add(EMIT_TOMBSTONE_ON_DELETE_SETTING);
        set.add(TOPIC_PREFIX_SETTING);
        set.add(PULSAR_SERVICE_URL_SETTING);
        set.add(KAFKA_SCHEMA_REGISTRY_URL_SETTING);
        set.add(KAFKA_BROKERS_SETTING);
        set.add(KAFKA_SCHEMA_REGISTRY_URL_SETTING);
        set.add(SSL_PROVIDER_SETTING);
        set.add(SSL_TRUSTSTORE_PATH_SETTING);
        set.add(SSL_TRUSTSTORE_PASSWORD_SETTING);
        set.add(SSL_TRUSTSTORE_TYPE_SETTING);
        set.add(SSL_KEYSTORE_PATH_SETTING);
        set.add(SSL_KEYSTORE_PASSWORD_SETTING);
        set.add(SSL_CIPHER_SUITES_SETTING);
        set.add(SSL_ENABLED_PROTOCOLS_SETTING);
        set.add(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_SETTING);
        set.add(SSL_ALLOW_INSECURE_CONNECTION_SETTING);
        set.add(SSL_HOSTNAME_VERIFICATION_ENABLE_SETTING);
        set.add(PULSAR_AUTH_PLUGIN_CLASS_NAME_SETTING);
        set.add(PULSAR_AUTH_PARAMS_SETTING);
        set.add(KAFKA_PROPERTIES_SETTINGS);
        settings = Collections.unmodifiableSet(set);

        Map<String, Setting<?>> map = new HashMap<>();
        settings.forEach(s -> map.put(s.name, s));
        settingMap = Collections.unmodifiableMap(map);
    }

    /**
     * Override the system properties with agent parameters.
     *
     * @param agentParameters
     */
    public static void configure(String agentParameters) {
        if (agentParameters != null) {
            for (String token : agentParameters.split("(?<!\\\\),\\s*")) {
                String param = token.replace("\\,", ",");
                int i = param.indexOf("=");
                if (i > 0) {
                    String key = param.substring(0, i);
                    String value = param.substring(i + 1);
                    Setting<?> setting = settingMap.get(key);
                    if (setting != null) {
                        setting.initializer.apply(value);
                    } else {
                        throw new RuntimeException(String.format("Unknown parameter '%s'", key));
                    }
                }
            }
        }
        if (log.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            settings.forEach(s -> {
                if (sb.length() > 0)
                    sb.append(", ");
                sb.append(s.name).append("=").append(s.supplier.get());
            });
            log.info(sb.toString());
        }
    }

    public static void configureKafkaTls(Properties props) {
        // TLS, see https://docs.confluent.io/platform/current/kafka/authentication_ssl.html#clients
        if (ProducerConfig.sslTruststorePath != null) {
            props.put("ssl.truststore.location", ProducerConfig.sslTruststorePath);
            props.put("ssl.truststore.password", ProducerConfig.sslTruststorePassword);
            props.put("ssl.truststore.type", ProducerConfig.sslTruststoreType);
        }
        if (ProducerConfig.sslKeystorePath != null) {
            props.put("ssl.keystore.location", ProducerConfig.sslKeystorePath);
            props.put("ssl.keystore.password", ProducerConfig.sslKeystorePassword);
        }
        if (ProducerConfig.sslProvider != null && ProducerConfig.sslProvider.length() > 0) {
            props.put("ssl.provider", ProducerConfig.sslProvider);
        }
        if (ProducerConfig.sslCipherSuites != null && ProducerConfig.sslCipherSuites.length() > 0) {
            props.put("ssl.cipher.suites", ProducerConfig.sslCipherSuites);
        }
        if (ProducerConfig.sslEnabledProtocols != null && ProducerConfig.sslEnabledProtocols.length() > 0) {
            props.put("ssl.enabled.protocols", ProducerConfig.sslEnabledProtocols);
        }
        if (ProducerConfig.sslEndpointIdentificationAlgorithm != null && ProducerConfig.sslEndpointIdentificationAlgorithm.length() > 0) {
            props.put("ssl.endpoint.identification.algorithm", ProducerConfig.sslEndpointIdentificationAlgorithm);
        }
    }
}
