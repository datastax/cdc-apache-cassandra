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
package com.datastax.cassandra.cdc.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class ProducerConfig {

    public static final String CDC_PROPERTY_PREFIX = "cdc.";
    public static final String storageDir = System.getProperty("cassandra.storagedir", null);

    public enum Platform {
        ALL, PULSAR, KAFKA;
    }

    @AllArgsConstructor
    public static class Setting<T> {
        public final String name;
        public final Platform platform;
        public final BiFunction<ProducerConfig, String, T> initializer;
        public final Function<ProducerConfig, T> supplier;
    }

    public static final String CDC_RELOCATION_DIR = "cdcRelocationDir";
    public String cdcRelocationDir = System.getProperty(CDC_PROPERTY_PREFIX + CDC_RELOCATION_DIR, storageDir + File.separator + "cdc_backup");
    public static final Setting<String> CDC_RELOCATION_DIR_SETTING =
            new Setting<>(CDC_RELOCATION_DIR, Platform.ALL, (c, s) -> c.cdcRelocationDir = s, c -> c.cdcRelocationDir);

    public static final String CDC_DIR_POOL_INTERVAL_MS = "cdcPoolIntervalMs";
    public long cdcDirPollIntervalMs = Long.getLong(CDC_PROPERTY_PREFIX + CDC_DIR_POOL_INTERVAL_MS, 60000L);
    public static final Setting<Long> CDC_DIR_POOL_INTERVAL_MS_SETTING =
            new Setting<>(CDC_DIR_POOL_INTERVAL_MS, Platform.ALL, (c, s) -> c.cdcDirPollIntervalMs = Long.parseLong(s), c -> c.cdcDirPollIntervalMs);

    public static final String ERROR_COMMITLOG_REPROCESS_ENABLED = "errorCommitLogReprocessEnabled";
    public boolean errorCommitLogReprocessEnabled = Boolean.getBoolean(CDC_PROPERTY_PREFIX + ERROR_COMMITLOG_REPROCESS_ENABLED);
    public static final Setting<Boolean> ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING =
            new Setting<>(ERROR_COMMITLOG_REPROCESS_ENABLED, Platform.ALL, (c, s) -> c.errorCommitLogReprocessEnabled = Boolean.parseBoolean(s), c -> c.errorCommitLogReprocessEnabled);

    public static final String EMIT_TOMBSTONE_ON_DELETE = "emitTombstoneOnDelete";
    public boolean emitTombstoneOnDelete = Boolean.getBoolean(CDC_PROPERTY_PREFIX + EMIT_TOMBSTONE_ON_DELETE);
    public static final Setting<Boolean> EMIT_TOMBSTONE_ON_DELETE_SETTING =
            new Setting<>(EMIT_TOMBSTONE_ON_DELETE, Platform.ALL, (c, s) -> c.emitTombstoneOnDelete = Boolean.parseBoolean(s), c -> c.emitTombstoneOnDelete);

    public static final String TOPIC_PREFIX = "topicPrefix";
    public String topicPrefix = System.getProperty(CDC_PROPERTY_PREFIX + TOPIC_PREFIX, "events-");
    public static final Setting<String> TOPIC_PREFIX_SETTING =
            new Setting<>(TOPIC_PREFIX, Platform.ALL, (c, s) -> c.topicPrefix = s, c -> c.topicPrefix);

    public static final String PULSAR_SERVICE_URL = "pulsarServiceUrl";
    public String pulsarServiceUrl = System.getProperty(CDC_PROPERTY_PREFIX + PULSAR_SERVICE_URL, "pulsar://localhost:6650");
    public static final Setting<String> PULSAR_SERVICE_URL_SETTING =
            new Setting<>(PULSAR_SERVICE_URL, Platform.PULSAR, (c, s) -> c.pulsarServiceUrl = s, c -> c.pulsarServiceUrl);

    public static final String KAFKA_BROKERS = "kafkaBrokers";
    public String kafkaBrokers = System.getProperty(CDC_PROPERTY_PREFIX + KAFKA_BROKERS, "localhost:9092");
    public static final Setting<String> KAFKA_BROKERS_SETTING =
            new Setting<>(KAFKA_BROKERS, Platform.KAFKA, (c, s) -> c.kafkaBrokers = s, c -> c.kafkaBrokers);

    public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafkaSchemaRegistryUrl";
    public String kafkaSchemaRegistryUrl = System.getProperty(CDC_PROPERTY_PREFIX + KAFKA_SCHEMA_REGISTRY_URL, "http://localhost:8081");
    public static final Setting<String> KAFKA_SCHEMA_REGISTRY_URL_SETTING =
            new Setting<>(KAFKA_SCHEMA_REGISTRY_URL, Platform.KAFKA, (c, s) -> c.kafkaSchemaRegistryUrl = s, c -> c.kafkaSchemaRegistryUrl);

    public static final String SSL_PROVIDER = "sslProvider";
    public String sslProvider = System.getProperty(CDC_PROPERTY_PREFIX + SSL_PROVIDER);
    public static final Setting<String> SSL_PROVIDER_SETTING =
            new Setting<>(SSL_PROVIDER, Platform.ALL, (c, s) -> c.sslProvider = s, c -> c.sslProvider);

    public static final String SSL_TRUSTSTORE_PATH = "sslTruststorePath";
    public String sslTruststorePath = System.getProperty(CDC_PROPERTY_PREFIX + SSL_TRUSTSTORE_PATH);
    public static final Setting<String> SSL_TRUSTSTORE_PATH_SETTING =
            new Setting<>(SSL_TRUSTSTORE_PATH, Platform.ALL, (c, s) -> c.sslTruststorePath = s, c -> c.sslTruststorePath);

    public static final String SSL_TRUSTSTORE_PASSWORD = "sslTruststorePassword";
    public String sslTruststorePassword = System.getProperty(CDC_PROPERTY_PREFIX + SSL_TRUSTSTORE_PASSWORD);
    public static final Setting<String> SSL_TRUSTSTORE_PASSWORD_SETTING =
            new Setting<>(SSL_TRUSTSTORE_PASSWORD, Platform.ALL, (c, s) -> c.sslTruststorePassword = s, c -> c.sslTruststorePassword);

    public static final String SSL_TRUSTSTORE_TYPE = "sslTruststoreType";
    public String sslTruststoreType = System.getProperty(CDC_PROPERTY_PREFIX + SSL_TRUSTSTORE_TYPE, "JKS");
    public static final Setting<String> SSL_TRUSTSTORE_TYPE_SETTING =
            new Setting<>(SSL_TRUSTSTORE_TYPE, Platform.ALL, (c, s) -> c.sslTruststoreType = s, c -> c.sslTruststoreType);

    public static final String SSL_KEYSTORE_PATH = "sslKeystorePath";
    public String sslKeystorePath = System.getProperty(CDC_PROPERTY_PREFIX + SSL_KEYSTORE_PATH);
    public static final Setting<String> SSL_KEYSTORE_PATH_SETTING =
            new Setting<>(SSL_KEYSTORE_PATH, Platform.ALL, (c, s) -> c.sslKeystorePath = s, c -> c.sslKeystorePath);

    public static final String SSL_KEYSTORE_PASSWORD = "sslKeystorePassword";
    public String sslKeystorePassword = System.getProperty(CDC_PROPERTY_PREFIX + SSL_KEYSTORE_PASSWORD);
    public static final Setting<String> SSL_KEYSTORE_PASSWORD_SETTING =
            new Setting<>(SSL_KEYSTORE_PASSWORD, Platform.ALL, (c, s) -> c.sslKeystorePassword = s, c -> c.sslKeystorePassword);

    public static final String SSL_CIPHER_SUITES = "sslCipherSuites";
    public String sslCipherSuites = System.getProperty(CDC_PROPERTY_PREFIX + SSL_CIPHER_SUITES);
    public static final Setting<String> SSL_CIPHER_SUITES_SETTING =
            new Setting<>(SSL_CIPHER_SUITES, Platform.ALL, (c, s) -> c.sslCipherSuites = s, c -> c.sslCipherSuites);

    public static final String SSL_ENABLED_PROTOCOLS = "sslEnabledProtocols";
    public String sslEnabledProtocols = System.getProperty(CDC_PROPERTY_PREFIX + SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1,TLSv1");
    public static final Setting<String> SSL_ENABLED_PROTOCOLS_SETTING =
            new Setting<>(SSL_ENABLED_PROTOCOLS, Platform.ALL, (c, s) -> c.sslEnabledProtocols = s, c -> c.sslEnabledProtocols);

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "sslEndpointIdentificationAlgorithm";
    public String sslEndpointIdentificationAlgorithm = System.getProperty(CDC_PROPERTY_PREFIX + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
    public static final Setting<String> SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_SETTING =
            new Setting<>(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, Platform.KAFKA, (c, s) -> c.sslEndpointIdentificationAlgorithm = s, c -> c.sslEndpointIdentificationAlgorithm);

    public static final String SSL_ALLOW_INSECURE_CONNECTION = "sslAllowInsecureConnection";
    public boolean sslAllowInsecureConnection = Boolean.getBoolean(CDC_PROPERTY_PREFIX + SSL_ALLOW_INSECURE_CONNECTION);
    public static final Setting<Boolean> SSL_ALLOW_INSECURE_CONNECTION_SETTING =
            new Setting<>(SSL_ALLOW_INSECURE_CONNECTION, Platform.PULSAR, (c, s) -> c.sslAllowInsecureConnection = Boolean.parseBoolean(s), c -> c.sslAllowInsecureConnection);

    public static final String SSL_HOSTNAME_VERIFICATION_ENABLE = "sslHostnameVerificationEnable";
    public boolean sslHostnameVerificationEnable = Boolean.getBoolean(CDC_PROPERTY_PREFIX + SSL_HOSTNAME_VERIFICATION_ENABLE);
    public static final Setting<Boolean> SSL_HOSTNAME_VERIFICATION_ENABLE_SETTING =
            new Setting<>(SSL_HOSTNAME_VERIFICATION_ENABLE, Platform.PULSAR, (c, s) -> c.sslHostnameVerificationEnable = Boolean.parseBoolean(s), c -> c.sslHostnameVerificationEnable);

    public static final String PULSAR_AUTH_PLUGIN_CLASS_NAME = "pulsarAuthPluginClassName";
    public String pulsarAuthPluginClassName = System.getProperty(CDC_PROPERTY_PREFIX + PULSAR_AUTH_PLUGIN_CLASS_NAME);
    public static final Setting<String> PULSAR_AUTH_PLUGIN_CLASS_NAME_SETTING =
            new Setting<>(PULSAR_AUTH_PLUGIN_CLASS_NAME, Platform.PULSAR, (c, s) -> c.pulsarAuthPluginClassName = s, c -> c.pulsarAuthPluginClassName);

    public static final String PULSAR_AUTH_PARAMS = "pulsarAuthParams";
    public String pulsarAuthParams = System.getProperty(CDC_PROPERTY_PREFIX + PULSAR_AUTH_PARAMS);
    public static final Setting<String> PULSAR_AUTH_PARAMS_SETTING =
            new Setting<>(PULSAR_AUTH_PARAMS, Platform.PULSAR, (c, s) -> c.pulsarAuthParams = s, c -> c.pulsarAuthParams);

    // generic properties for kafka client
    public static final String KAFKA_PROPERTIES = "kafkaProperties";
    public Map<String, String> kafkaProperties = new HashMap<>();
    public static final Setting<Map<String, String>> KAFKA_PROPERTIES_SETTINGS =
            new Setting<>(KAFKA_PROPERTIES, Platform.KAFKA,
                    (c,s) -> {
                        for (String param : s.split(",")) {
                            int i = param.indexOf("=");
                            if (i > 0) {
                                c.kafkaProperties.put(param.substring(0, i), param.substring(i + 1));
                            }
                        }
                        return c.kafkaProperties;
                    },
                    c -> c.kafkaProperties);

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

    public static ProducerConfig create(Platform platform, Map<String, Object> tenantConfiguration)
    {
        return new ProducerConfig().configure(platform, tenantConfiguration);
    }

    public static ProducerConfig create(Platform platform, String agentParams)
    {
        return new ProducerConfig().configure(platform, agentParams);
    }

    /**
     * Override the system properties with agent parameters.
     *
     * @param agentParameters
     */
    public ProducerConfig configure(Platform platform, String agentParameters) {
        Map<String, Object> parameters = new HashMap<>();
        if (agentParameters != null) {
            for (String token : agentParameters.split("(?<!\\\\),\\s*")) {
                String param = token.replace("\\,", ",");
                int i = param.indexOf("=");
                if (i > 0) {
                    String key = param.substring(0, i);
                    String value = param.substring(i + 1);
                    parameters.put(key, value);
                }
            }
        }
        return configure(platform, parameters);
    }

    /**
     * Override the system properties with agent parameters.
     *
     * @param agentParameters
     */
    public ProducerConfig configure(Platform platform, Map<String, Object> agentParameters) {
        if (agentParameters == null) {
            agentParameters = new HashMap<>();
        }
        for (Map.Entry<String, Object> entry : agentParameters.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() == null) {
                continue;
            }
            if (! (entry.getValue() instanceof String)) {
                throw new IllegalArgumentException(String.format("Unsupported parameter '%s' of type, only String values are allowed ", key, entry.getValue().getClass()));
            }
            String value = (String) entry.getValue();
            Setting<?> setting = settingMap.get(key);
            if (setting != null) {
                if (!setting.platform.equals(Platform.ALL) && !setting.platform.equals(platform)) {
                    throw new IllegalArgumentException(String.format("Unsupported parameter '%s' for the %s platform ", key, platform));
                }
                setting.initializer.apply(this, value);
            } else {
                throw new RuntimeException(String.format("Unknown parameter '%s'", key));
            }
        }

        if (log.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            settings.forEach(s -> {
                if (s.platform.equals(Platform.ALL) || s.platform.equals(platform)) {
                    if (sb.length() > 0)
                        sb.append(", ");
                    sb.append(s.name).append("=").append(s.supplier.apply(this));
                }
            });
            log.info(sb.toString());
        }
        return this;
    }

    public void configureKafkaTls(Properties props) {
        // TLS, see https://docs.confluent.io/platform/current/kafka/authentication_ssl.html#clients
        if (sslTruststorePath != null) {
            props.put("ssl.truststore.location",sslTruststorePath);
            props.put("ssl.truststore.password", sslTruststorePassword);
            props.put("ssl.truststore.type", sslTruststoreType);
        }
        if (sslKeystorePath != null) {
            props.put("ssl.keystore.location", sslKeystorePath);
            props.put("ssl.keystore.password", sslKeystorePassword);
        }
        if (sslProvider != null && sslProvider.length() > 0) {
            props.put("ssl.provider", sslProvider);
        }
        if (sslCipherSuites != null && sslCipherSuites.length() > 0) {
            props.put("ssl.cipher.suites", sslCipherSuites);
        }
        if (sslEnabledProtocols != null && sslEnabledProtocols.length() > 0) {
            props.put("ssl.enabled.protocols", sslEnabledProtocols);
        }
        if (sslEndpointIdentificationAlgorithm != null && sslEndpointIdentificationAlgorithm.length() > 0) {
            props.put("ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
        }
    }
}
