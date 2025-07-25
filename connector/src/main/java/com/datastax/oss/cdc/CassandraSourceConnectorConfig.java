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

import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.SslConfig;
import com.datastax.oss.common.sink.util.SinkUtil;
import com.datastax.oss.common.sink.util.StringUtil;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.google.common.base.Splitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.*;

/**
 * Connector configuration and validation.
 */
@SuppressWarnings("WeakerAccess")
@Slf4j
public class CassandraSourceConnectorConfig {

    public static final String KEYSPACE_NAME_CONFIG = "keyspace";
    public static final String TABLE_NAME_CONFIG = "table";
    public static final String COLUMNS_REGEXP_CONFIG = "columns";

    public static final String EVENTS_TOPIC_NAME_CONFIG = "events.topic";
    public static final String EVENTS_SUBSCRIPTION_NAME_CONFIG = "events.subscription.name";
    public static final String EVENTS_SUBSCRIPTION_TYPE_CONFIG = "events.subscription.type";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String QUERY_EXECUTORS_CONFIG = "query.executors";
    public static final String QUERY_MAX_MOBILE_AVG_LATENCY_CONFIG = "query.maxMobileAvgLatency";
    public static final String QUERY_MIN_MOBILE_AVG_LATENCY_CONFIG = "query.minMobileAvgLatency";
    public static final String QUERY_BACKOFF_IN_MS_CONFIG = "query.backoffInMs";
    public static final String QUERY_MAX_BACKOFF_IN_SEC_CONFIG = "query.maxBackoffInSec";

    public static final String CACHE_ONLY_IF_COORDINATOR_MATCH = "cache.only_if_coordinator_match";
    public static final String CACHE_MAX_DIGESTS_CONFIG = "cache.max.digest";
    public static final String CACHE_MAX_CAPACITY_CONFIG = "cache.max.capacity";
    public static final String CACHE_EXPIRE_AFTER_MS_CONFIG = "cache.expire.after.ms";
    public static final String CACHE_PERSISTENT_DIRECTORY_CONFIG = "cache.persistent.directory";


    public static final String KEY_CONVERTER_CLASS_CONFIG = "key.converter";
    public static final String VALUE_CONVERTER_CLASS_CONFIG = "value.converter";

    public static final String DRIVER_CONFIG_PREFIX = "datastax-java-driver";

    static final String SSL_OPT_PREFIX = "ssl.";
    private static final String AUTH_OPT_PREFIX = "auth.";

    public static final String CONTACT_POINTS_OPT = "contactPoints";

    public static final String PORT_OPT = "port";

    public static final String DC_OPT = "loadBalancing.localDc";

    public static final String OUTPUT_FORMAT = "outputFormat";
    static final String LOCAL_DC_DRIVER_SETTING =
            withDriverPrefix(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);

    public static final String CONCURRENT_REQUESTS_OPT = "maxConcurrentRequests";

    public static final String QUERY_EXECUTION_TIMEOUT_OPT = "queryExecutionTimeout";
    static final String QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING =
            withDriverPrefix(DefaultDriverOption.REQUEST_TIMEOUT);
    public static final String QUERY_EXECUTION_TIMEOUT_DEFAULT = "30 seconds";

    public static final String CONNECTION_POOL_LOCAL_SIZE = "connectionPoolLocalSize";
    static final String CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING =
            withDriverPrefix(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE);
    public static final String CONNECTION_POOL_LOCAL_SIZE_DEFAULT = "4";

    static final String JMX_OPT = "jmx";
    public static final String JMX_CONNECTOR_DOMAIN_OPT = "jmxConnectorDomain";
    public static final String JMX_CONNECTOR_DOMAIN_OPT_DEFAULT = "com.datastax.oss.cdc";
    static final String COMPRESSION_OPT = "compression";
    static final String COMPRESSION_DRIVER_SETTING =
            withDriverPrefix(DefaultDriverOption.PROTOCOL_COMPRESSION);
    public static final String COMPRESSION_DEFAULT = "none";

    static final String METRICS_HIGHEST_LATENCY_OPT = "metricsHighestLatency";
    static final String METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS =
            withDriverPrefix(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST);
    static final String METRICS_HIGHEST_LATENCY_DEFAULT = "35 seconds";

    static final String IGNORE_ERRORS = "ignoreErrors";

    public static final String SECURE_CONNECT_BUNDLE_OPT = "cloud.secureConnectBundle";
    static final String SECURE_CONNECT_BUNDLE_DRIVER_SETTING =
            withDriverPrefix(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE);

    static final List<String> JAVA_DRIVER_SETTINGS_LIST_TYPE =
            ImmutableList.of(
                    withDriverPrefix(METRICS_SESSION_ENABLED),
                    withDriverPrefix(CONTACT_POINTS),
                    withDriverPrefix(METADATA_SCHEMA_REFRESHED_KEYSPACES),
                    withDriverPrefix(METRICS_NODE_ENABLED),
                    withDriverPrefix(SSL_CIPHER_SUITES));

    public static final ConfigDef GLOBAL_CONFIG_DEF =
            new ConfigDef()
                    .define(KEYSPACE_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.Importance.HIGH,
                            "Cassandra keyspace name")
                    .define(TABLE_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.Importance.HIGH,
                            "Cassandra table name")
                    .define(COLUMNS_REGEXP_CONFIG,
                            ConfigDef.Type.STRING,
                            ".*",
                            ConfigDef.Importance.LOW,
                            "Regular expression of the Cassandra replicated column names")
                    .define(EVENTS_TOPIC_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.Importance.HIGH,
                            "The topic name to listen cassandra mutation events to")
                    .define(EVENTS_SUBSCRIPTION_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            "sub",
                            ConfigDef.Importance.HIGH,
                            "The pulsar events topic subscription name, with a default set to 'sub'",
                            "Pulsar only", 1, ConfigDef.Width.NONE, "SubscriptionName")
                    .define(EVENTS_SUBSCRIPTION_TYPE_CONFIG,
                            ConfigDef.Type.STRING,
                            "Key_Shared",
                            ConfigDef.ValidString.in("Exclusive", "Shared", "Failover", "Key_Shared"),
                            ConfigDef.Importance.HIGH,
                            "The pulsar events topic subscription type, with a default set to Key_Shared (case sensitive) for a non-partitioned events topic." +
                            " If your events topic is partitioned, you should set subscription type to Failover",
                            "Pulsar only", 2, ConfigDef.Width.NONE, "SubscriptionType")
                    .define(BATCH_SIZE_CONFIG,
                            ConfigDef.Type.INT,
                            200,
                            ConfigDef.Importance.MEDIUM,
                            "The batch size for grouping mutations before sending them to the data topic")
                    .define(QUERY_EXECUTORS_CONFIG,
                            ConfigDef.Type.INT,
                            10,
                            ConfigDef.Importance.MEDIUM,
                            "The initial and maximum number of threads to execute concurrent Cassandra queries")
                    .define(QUERY_MAX_MOBILE_AVG_LATENCY_CONFIG,
                            ConfigDef.Type.LONG,
                            100L,
                            ConfigDef.Importance.MEDIUM,
                            "Maximum mobile average CQL query latency beyond which the number of executors is decreased")
                    .define(QUERY_MIN_MOBILE_AVG_LATENCY_CONFIG,
                            ConfigDef.Type.LONG,
                            10L,
                            ConfigDef.Importance.MEDIUM,
                            "Minimum mobile average CQL query latency beyond which the number of executors is increased")
                    .define(QUERY_BACKOFF_IN_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            100L,
                            ConfigDef.Importance.MEDIUM,
                            "Retry backoff in milliseconds when there is not enough Cassandra replicas to perform the query. " +
                                    "(Capped exponential jittered backoff)")
                    .define(QUERY_MAX_BACKOFF_IN_SEC_CONFIG,
                            ConfigDef.Type.LONG,
                            3600L,
                            ConfigDef.Importance.MEDIUM,
                            "Maximum backoff delay in seconds when there is not enough Cassandra replicas to perform the query")
                    .define(CACHE_MAX_DIGESTS_CONFIG,
                            ConfigDef.Type.LONG,
                            "3",
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "The maximum number of digest per mutation cache entry, with a default set to 3",
                            "CQL Read cache", 1, ConfigDef.Width.NONE, "CacheMaxDigest")
                    .define(CACHE_PERSISTENT_DIRECTORY_CONFIG,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.HIGH,
                            "The directory where the persistent mutation cache will be stored. Formated as `{cache.persistent.directory}/{source-name}-{instance-id}` " +
                                    "If set, the connector will use RocksDB to store digests, otherwise it will use an in-memory cache.",
                            "CQL Read cache", 1, ConfigDef.Width.NONE, "CachePersistentDirectory")
                    .define(CACHE_MAX_CAPACITY_CONFIG,
                            ConfigDef.Type.LONG,
                            "32767",
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "The maximum capacity of the mutation cache, with a default size of 32767",
                            "CQL Read cache", 2, ConfigDef.Width.NONE, "CacheMaxDigest")
                    .define(CACHE_EXPIRE_AFTER_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            "60000",
                            ConfigDef.Range.atLeast(1000),
                            ConfigDef.Importance.HIGH,
                            "The mutation cache entry duration in milliseconds, with a default value of 60 seconds.",
                            "CQL Read cache", 3, ConfigDef.Width.NONE, "CacheExpireAfter")
                    .define(CACHE_ONLY_IF_COORDINATOR_MATCH,
                            ConfigDef.Type.BOOLEAN,
                            "true",
                            ConfigDef.Importance.HIGH,
                            "Cache the mutation digest only if the coordinator node is the originator node.",
                            "CQL Read cache", 4, ConfigDef.Width.NONE, "CacheExpireAfter")
                    .define(KEY_CONVERTER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Converter class used to write the message key to the data topic. This setting is experimental for advanced user only.")
                    .define(VALUE_CONVERTER_CLASS_CONFIG,
                            ConfigDef.Type.CLASS,
                            null,
                            ConfigDef.Importance.HIGH,
                            "Converter class used to write the message value to the data topic. This setting is experimental for advanced user only.")
                    .define(
                            CassandraSinkConfig.CONTACT_POINTS_OPT,
                            ConfigDef.Type.LIST,
                            Collections.EMPTY_LIST,
                            ConfigDef.Importance.HIGH,
                            "Initial contact points")
                    .define(
                            PORT_OPT,
                            ConfigDef.Type.INT,
                            9042,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "Port to connect to nodes")
                    .define(
                            DC_OPT,
                            ConfigDef.Type.STRING,
                            "",
                            ConfigDef.Importance.HIGH,
                            "The datacenter name (commonly dc1, dc2, etc.) local to the machine on which the connector is running")
                    .define(
                            CONCURRENT_REQUESTS_OPT,
                            ConfigDef.Type.INT,
                            500,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "The maximum number of requests to send at once")
                    .define(
                            JMX_OPT,
                            ConfigDef.Type.BOOLEAN,
                            true,
                            ConfigDef.Importance.HIGH,
                            "Whether to enable JMX reporting")
                    .define(
                            JMX_CONNECTOR_DOMAIN_OPT,
                            ConfigDef.Type.STRING,
                            JMX_CONNECTOR_DOMAIN_OPT_DEFAULT,
                            ConfigDef.Importance.LOW,
                            "Domain for JMX reporting")
                    .define(
                            COMPRESSION_OPT,
                            ConfigDef.Type.STRING,
                            "None",
                            ConfigDef.CaseInsensitiveValidString.in("NONE", "LZ4", "SNAPPY"),
                            ConfigDef.Importance.HIGH,
                            "Compression algorithm to use when issuing requests to the database server")
                    .define(
                            QUERY_EXECUTION_TIMEOUT_OPT,
                            ConfigDef.Type.INT,
                            30,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "CQL statement execution timeout, in seconds")
                    .define(
                            METRICS_HIGHEST_LATENCY_OPT,
                            ConfigDef.Type.INT,
                            35,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "This is used to scale internal data structures for gathering metrics. "
                                    + "It should be higher than queryExecutionTimeout. This parameter should be expressed in seconds.")
                    .define(
                            CONNECTION_POOL_LOCAL_SIZE,
                            ConfigDef.Type.INT,
                            4,
                            ConfigDef.Range.atLeast(1),
                            ConfigDef.Importance.HIGH,
                            "Number of connections that driver maintains within a connection pool to each node in local dc")
                    .define(
                            IGNORE_ERRORS,
                            ConfigDef.Type.STRING,
                            "None",
                            ConfigDef.Importance.HIGH,
                            "Specifies which errors the connector should ignore when processing the record. "
                                    + "Valid values are: "
                                    + "None (never ignore errors), "
                                    + "All (ignore all errors), "
                                    + "Driver (ignore driver errors only, i.e. errors when writing to the database).")
                    .define(
                            SECURE_CONNECT_BUNDLE_OPT,
                            ConfigDef.Type.STRING,
                            "",
                            ConfigDef.Importance.HIGH,
                            "The location of the cloud secure bundle used to connect to Datastax Astra DB.")
                    .define(OUTPUT_FORMAT,
                            ConfigDef.Type.STRING,
                            "key-value-avro",
                            ConfigDef.ValidString.in("key-value-avro", "key-value-json", "json"),
                            ConfigDef.Importance.LOW,
                            "The format of the messages on the data topic. "
                                    + "Valid values are: "
                                    + "key-value-avro (encodes the key and value separately, both in AVRO format), "
                                    + "key-value-json (encodes the key and value separately, both in JSON format), "
                                    + "json (key and value are encoded together in single JSON object)" );
    private static final Function<String, String> TO_SECONDS_CONVERTER =
            v -> String.format("%s seconds", v);

    static final String METRICS_INTERVAL_DEFAULT = "30 seconds";

    private final String instanceName;
    private final AbstractConfig globalConfig;
    private final Map<String, String> javaDriverSettings;

    @Nullable
    private SslConfig sslConfig;

    private final AuthenticatorConfig authConfig;

    public CassandraSourceConnectorConfig(Map<String, String> settings) {
        try {
            log.debug("create CassandraSourceConfig for settings:{} ", settings);
            instanceName = settings.get(SinkUtil.NAME_OPT);
            // Walk through the settings and separate out "globals" from "topics", "ssl", and "auth".
            Map<String, String> globalSettings = new HashMap<>();
            Map<String, String> sslSettings = new HashMap<>();
            Map<String, String> authSettings = new HashMap<>();
            Map<String, Map<String, String>> topicSettings = new HashMap<>();
            javaDriverSettings = new HashMap<>();
            for (Map.Entry<String, String> entry : settings.entrySet()) {
                String name = entry.getKey();
                if (name.startsWith(SSL_OPT_PREFIX)) {
                    sslSettings.put(name, entry.getValue());
                } else if (name.startsWith(AUTH_OPT_PREFIX)) {
                    authSettings.put(name, entry.getValue());
                } else if (name.startsWith(DRIVER_CONFIG_PREFIX)) {
                    addJavaDriverSetting(entry);
                } else {
                    globalSettings.put(name, entry.getValue());
                }
            }

            // Put the global settings in an AbstractConfig and make/store a TopicConfig for every
            // topic settings map.
            globalConfig = new AbstractConfig(GLOBAL_CONFIG_DEF, globalSettings, false);

            populateDriverSettingsWithConnectorSettings(globalSettings);

            // for Pulsar Sink we want to make it easy to deploy these files
            // as simple base64 encoded strings, because there is no
            // automatic mechanism to distribute files to the workers that
            // execute the Sink
            decodeBase64EncodedFile(SECURE_CONNECT_BUNDLE_DRIVER_SETTING, javaDriverSettings);
            decodeBase64EncodedFile(SslConfig.KEYSTORE_PATH_OPT, sslSettings);
            decodeBase64EncodedFile(SslConfig.TRUSTSTORE_PATH_OPT, sslSettings);
            decodeBase64EncodedFile(SslConfig.OPENSSL_PRIVATE_KEY_OPT, sslSettings);
            decodeBase64EncodedFile(SslConfig.OPENSSL_KEY_CERT_CHAIN_OPT, sslSettings);
            decodeBase64EncodedFile(AuthenticatorConfig.KEYTAB_OPT, authSettings);

            boolean cloud = isCloud();

            if (!cloud) {
                sslConfig = new SslConfig(sslSettings);
            }
            authConfig = new AuthenticatorConfig(authSettings);

            validateCompressionType();

            if (cloud) {
                // Verify that if cloudSecureBundle specified the
                // other clashing properties (contactPoints, dc, ssl) are not set.
                validateCloudSettings(sslSettings);
            }

            // Verify that if contact-points are provided, local dc is also specified.
            List<String> contactPoints = getContactPoints();
            log.debug("contactPoints: {}", contactPoints);
            if (!contactPoints.isEmpty() && !getLocalDc().isPresent()) {
                throw new ConfigException(
                        CassandraSinkConfig.CONTACT_POINTS_OPT,
                        contactPoints,
                        String.format("When contact points is provided, %s must also be specified", DC_OPT));
            }
        } catch (org.apache.kafka.common.config.ConfigException err) {
            // convert Kafka config framework exception into our exception
            throw new ConfigException(err.getMessage(), err);
        }
    }

    private static final Splitter COMA_SPLITTER = Splitter.on(",");

    private void addJavaDriverSetting(Map.Entry<String, String> entry) {

        if (JAVA_DRIVER_SETTINGS_LIST_TYPE.contains(entry.getKey())) {
            putAsTypesafeListProperty(entry.getKey(), entry.getValue());
        } else {
            javaDriverSettings.put(entry.getKey(), entry.getValue());
        }
    }

    private void putAsTypesafeListProperty(@NonNull String key, @NonNull String value) {
        List<String> values = COMA_SPLITTER.splitToList(value);
        for (int i = 0; i < values.size(); i++) {
            javaDriverSettings.put(String.format("%s.%d", key, i), values.get(i).trim());
        }
    }

    private void validateCompressionType() {
        String compressionTypeValue = javaDriverSettings.get(COMPRESSION_DRIVER_SETTING);
        if (!("none".equalsIgnoreCase(compressionTypeValue)
                || "snappy".equalsIgnoreCase(compressionTypeValue)
                || "lz4".equalsIgnoreCase(compressionTypeValue))) {
            throw new ConfigException(
                    COMPRESSION_OPT, compressionTypeValue, "valid values are none, snappy, lz4");
        }
    }

    private void populateDriverSettingsWithConnectorSettings(Map<String, String> connectorSettings) {
        deprecatedLocalDc(connectorSettings);
        deprecatedConnectionPoolSize(connectorSettings);
        deprecatedQueryExecutionTimeout(connectorSettings);
        deprecatedMetricsHighestLatency(connectorSettings);
        deprecatedCompression(connectorSettings);
        deprecatedSecureBundle(connectorSettings);
        if (getJmx()) {
            metricsSettings();
        }
    }

    private void deprecatedSecureBundle(Map<String, String> connectorSettings) {
        handleDeprecatedSetting(
                connectorSettings,
                SECURE_CONNECT_BUNDLE_OPT,
                SECURE_CONNECT_BUNDLE_DRIVER_SETTING,
                null,
                Function.identity());
    }

    static void decodeBase64EncodedFile(String key, Map<String, String> javaDriverSettings) {
        // if the path is base64:xxxx we decode the payload and create
        // a temporary file
        // we are setting permissions such that only current user can access the file
        // the file will be deleted at JVM exit
        String encoded = javaDriverSettings.get(key);
        if (encoded != null && encoded.startsWith("base64:")) {
            try {
                encoded = encoded.replace("\n", "").replace("\r", "").trim();
                encoded = encoded.substring("base64:".length());
                byte[] decoded = Base64.getDecoder().decode(encoded);
                Path file = Files.createTempFile("cassandra.sink.", ".tmp");
                Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
                file.toFile().deleteOnExit();
                Files.write(file, decoded);
                String path = file.toAbsolutePath().toString();
                log.info("Decoded {} to temporary file {}", key, path);
                javaDriverSettings.put(key, path);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Cannot decode base64 " + key + " and create temporary file: " + ex, ex);
            }
        }
    }

    private void deprecatedCompression(Map<String, String> connectorSettings) {
        handleDeprecatedSetting(
                connectorSettings,
                COMPRESSION_OPT,
                COMPRESSION_DRIVER_SETTING,
                COMPRESSION_DEFAULT,
                Function.identity());
    }

    private void deprecatedLocalDc(Map<String, String> connectorSettings) {
        handleDeprecatedSetting(
                connectorSettings, DC_OPT, LOCAL_DC_DRIVER_SETTING, null, Function.identity());
    }

    private void deprecatedConnectionPoolSize(Map<String, String> connectorSettings) {
        handleDeprecatedSetting(
                connectorSettings,
                CONNECTION_POOL_LOCAL_SIZE,
                CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING,
                CONNECTION_POOL_LOCAL_SIZE_DEFAULT,
                Function.identity());
    }

    private void deprecatedQueryExecutionTimeout(Map<String, String> connectorSettings) {
        handleDeprecatedSetting(
                connectorSettings,
                QUERY_EXECUTION_TIMEOUT_OPT,
                QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING,
                QUERY_EXECUTION_TIMEOUT_DEFAULT,
                TO_SECONDS_CONVERTER);
    }

    private void deprecatedMetricsHighestLatency(Map<String, String> connectorSettings) {
        handleDeprecatedSetting(
                connectorSettings,
                METRICS_HIGHEST_LATENCY_OPT,
                METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS,
                METRICS_HIGHEST_LATENCY_DEFAULT,
                TO_SECONDS_CONVERTER);
    }

    private void metricsSettings() {
        String metricsEnabledDriverSetting = withDriverPrefix(METRICS_SESSION_ENABLED);

        // if user explicitly provided setting under datastax-java-driver do not add defaults
        if (javaDriverSettings
                .keySet()
                .stream()
                .noneMatch(v -> v.startsWith(metricsEnabledDriverSetting))) {
            javaDriverSettings.put(metricsEnabledDriverSetting + ".0", "cql-requests");
            javaDriverSettings.put(metricsEnabledDriverSetting + ".1", "cql-client-timeouts");
        }

        String sessionCqlRequestIntervalDriverSetting =
                withDriverPrefix(METRICS_SESSION_CQL_REQUESTS_INTERVAL);
        if (!javaDriverSettings.containsKey(sessionCqlRequestIntervalDriverSetting)) {
            javaDriverSettings.put(sessionCqlRequestIntervalDriverSetting, METRICS_INTERVAL_DEFAULT);
        }
    }

    private void handleDeprecatedSetting(
            @NonNull Map<String, String> connectorSettings,
            @NonNull String connectorDeprecatedSetting,
            @NonNull String driverSetting,
            @Nullable String defaultValue,
            @NonNull Function<String, String> deprecatedValueConverter) {
        // handle usage of deprecated setting
        if (connectorSettings.containsKey(connectorDeprecatedSetting)) {
            // put or override if setting with datastax-java-driver prefix provided
            javaDriverSettings.put(
                    driverSetting,
                    deprecatedValueConverter.apply(connectorSettings.get(connectorDeprecatedSetting)));
        }

        if (defaultValue != null) {
            // handle default if setting is not provided
            if (!javaDriverSettings.containsKey(driverSetting)) {
                javaDriverSettings.put(driverSetting, defaultValue);
            }
        }
    }

    public static String withDriverPrefix(DefaultDriverOption option) {
        return String.format("%s.%s", DRIVER_CONFIG_PREFIX, option.getPath());
    }

    private void validateCloudSettings(Map<String, String> sslSettings) {
        if (!getContactPoints().isEmpty()) {
            throw new ConfigException(
                    String.format(
                            "When %s parameter is specified you should not provide %s.",
                            SECURE_CONNECT_BUNDLE_OPT, CassandraSinkConfig.CONTACT_POINTS_OPT));
        }

        if (getLocalDc().isPresent()) {
            throw new ConfigException(
                    String.format(
                            "When %s parameter is specified you should not provide %s.",
                            SECURE_CONNECT_BUNDLE_OPT, DC_OPT));
        }

        if (!sslSettings.isEmpty()) {
            throw new ConfigException(
                    String.format(
                            "When %s parameter is specified you should not provide any setting under %s.",
                            SECURE_CONNECT_BUNDLE_OPT, SSL_OPT_PREFIX));
        }
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getEventsSubscriptionName() {
        return globalConfig.getString(EVENTS_SUBSCRIPTION_NAME_CONFIG);
    }

    public String getKeyspaceName() {
        return globalConfig.getString(KEYSPACE_NAME_CONFIG);
    }

    public String getTableName() {
        return globalConfig.getString(TABLE_NAME_CONFIG);
    }

    public String getColumnsRegexp() {
        return globalConfig.getString(COLUMNS_REGEXP_CONFIG);
    }

    public String getEventsTopic() {
        return globalConfig.getString(EVENTS_TOPIC_NAME_CONFIG);
    }

    public Class<?> getKeyConverterClass() {
        return globalConfig.getClass(KEY_CONVERTER_CLASS_CONFIG);
    }

    public Class<?> getValueConverterClass() {
        return globalConfig.getClass(VALUE_CONVERTER_CLASS_CONFIG);
    }

    public int getBatchSize() {
        return globalConfig.getInt(BATCH_SIZE_CONFIG);
    }

    public String getEventsSubscriptionType() {
        return globalConfig.getString(EVENTS_SUBSCRIPTION_TYPE_CONFIG);
    }

    public int getQueryExecutors() {
        return globalConfig.getInt(QUERY_EXECUTORS_CONFIG);
    }

    public int getPort() {
        return globalConfig.getInt(PORT_OPT);
    }

    public int getMaxConcurrentRequests() {
        return globalConfig.getInt(CONCURRENT_REQUESTS_OPT);
    }

    public long getQueryMaxMobileAvgLatency() {
        return globalConfig.getLong(QUERY_MAX_MOBILE_AVG_LATENCY_CONFIG);
    }

    public long getQueryMinMobileAvgLatency() {
        return globalConfig.getLong(QUERY_MIN_MOBILE_AVG_LATENCY_CONFIG);
    }

    public long getQueryBackoffInMs() {
        return globalConfig.getLong(QUERY_BACKOFF_IN_MS_CONFIG);
    }

    public long getQueryMaxBackoffInSec() {
        return globalConfig.getLong(QUERY_MAX_BACKOFF_IN_SEC_CONFIG);
    }

    public boolean getCacheOnlyIfCoordinatorMatch() {
        return globalConfig.getBoolean(CACHE_ONLY_IF_COORDINATOR_MATCH);
    }

    public long getCacheMaxDigests() {
        return globalConfig.getLong(CACHE_MAX_DIGESTS_CONFIG);
    }

    public String getCachePersistentDirectory() {
        return globalConfig.getString(CACHE_PERSISTENT_DIRECTORY_CONFIG);
    }

    public long getCacheMaxCapacity() {
        return globalConfig.getLong(CACHE_MAX_CAPACITY_CONFIG);
    }

    public long getCacheExpireAfterMs() {
        return globalConfig.getLong(CACHE_EXPIRE_AFTER_MS_CONFIG);
    }

    public enum IgnoreErrorsPolicy {
        ALL,
        NONE,
        DRIVER
    }

    public enum OutputFormat {
        KEY_VALUE_AVRO,
        KEY_VALUE_JSON,
        JSON // Both key and value are encoded in the message value. The message key is also populated with a JSON string.
    }

    public IgnoreErrorsPolicy getIgnoreErrors() {
        String ignoreErrors = globalConfig.getString(IGNORE_ERRORS);
        if ("none".equalsIgnoreCase(ignoreErrors)) {
            return IgnoreErrorsPolicy.NONE;
        } else if ("all".equalsIgnoreCase(ignoreErrors)) {
            return IgnoreErrorsPolicy.ALL;
        } else if ("driver".equalsIgnoreCase(ignoreErrors)) {
            return IgnoreErrorsPolicy.DRIVER;
        } else if ("false".equalsIgnoreCase(ignoreErrors)) {
            log.warn(
                    "Setting {}=false is deprecated, please replace with {}=None",
                    IGNORE_ERRORS,
                    IGNORE_ERRORS);
            return IgnoreErrorsPolicy.NONE;
        } else if ("true".equalsIgnoreCase(ignoreErrors)) {
            log.warn(
                    "Setting {}=true is deprecated, please replace with {}=Driver",
                    IGNORE_ERRORS,
                    IGNORE_ERRORS);
            return IgnoreErrorsPolicy.DRIVER;
        }
        throw new IllegalArgumentException(
                "Invalid value for setting "
                        + IGNORE_ERRORS
                        + ", expecting either All, None or Driver, got: "
                        + ignoreErrors);
    }

    public boolean getJmx() {
        return globalConfig.getBoolean(JMX_OPT);
    }

    public String getJmxConnectorDomain() {
        return globalConfig.getString(JMX_CONNECTOR_DOMAIN_OPT);
    }

    public boolean isCloud() {
        return !StringUtil.isEmpty(javaDriverSettings.get(SECURE_CONNECT_BUNDLE_DRIVER_SETTING));
    }

    public List<String> getContactPoints() {
        return globalConfig.getList(CassandraSinkConfig.CONTACT_POINTS_OPT);
    }

    public Optional<String> getLocalDc() {
        return Optional.ofNullable(javaDriverSettings.get(LOCAL_DC_DRIVER_SETTING))
                .filter(v -> !v.isEmpty());
    }

    public AuthenticatorConfig getAuthenticatorConfig() {
        return authConfig;
    }

    public OutputFormat getOutputFormat() {
        switch (globalConfig.getString(OUTPUT_FORMAT)) {
            case "key-value-avro":
                return OutputFormat.KEY_VALUE_AVRO;
            case "key-value-json":
                return OutputFormat.KEY_VALUE_JSON;
            case "json":
                return OutputFormat.JSON;
            default:
                throw new IllegalArgumentException("Illegal output format: " + globalConfig.getString(OUTPUT_FORMAT));
        }
    }

    public boolean isAvroOutputFormat() {
        return getOutputFormat() == OutputFormat.KEY_VALUE_AVRO;
    }

    public boolean isJsonOutputFormat() {
        OutputFormat format = getOutputFormat();
        return format == OutputFormat.KEY_VALUE_JSON || format == OutputFormat.JSON;
    }

    public boolean isJsonOnlyOutputFormat() {
        return getOutputFormat() == OutputFormat.JSON;
    }

    @Nullable
    public SslConfig getSslConfig() {
        return sslConfig;
    }

    @Override
    public String toString() {
        return String.format(
                "Global configuration:%n"
                        + "        " + KEYSPACE_NAME_CONFIG + ": %s%n"
                        + "        " + TABLE_NAME_CONFIG + ": %s%n"
                        + "        " + COLUMNS_REGEXP_CONFIG + ": %s%n"
                        + "        " + EVENTS_TOPIC_NAME_CONFIG + ": %s%n"
                        + "        " + EVENTS_SUBSCRIPTION_NAME_CONFIG + ": %s%n"
                        + "        " + EVENTS_SUBSCRIPTION_TYPE_CONFIG + ": %s%n"
                        + "        " + BATCH_SIZE_CONFIG + ": %d%n"
                        + "        " + QUERY_EXECUTORS_CONFIG + ": %d%n"
                        + "        " + QUERY_MIN_MOBILE_AVG_LATENCY_CONFIG + ": %d%n"
                        + "        " + QUERY_MAX_MOBILE_AVG_LATENCY_CONFIG + ": %d%n"
                        + "        " + QUERY_BACKOFF_IN_MS_CONFIG + ": %d%n"
                        + "        " + QUERY_MAX_BACKOFF_IN_SEC_CONFIG + ": %d%n"
                        + "        " + CACHE_MAX_DIGESTS_CONFIG + ": %d%n"
                        + "        " + CACHE_PERSISTENT_DIRECTORY_CONFIG + ": %s%n"
                        + "        " + CACHE_MAX_CAPACITY_CONFIG + ": %d%n"
                        + "        " + CACHE_EXPIRE_AFTER_MS_CONFIG + ": %d%n"
                        + "        " + CACHE_ONLY_IF_COORDINATOR_MATCH + ": %s%n"
                        + "        contactPoints: %s%n"
                        + "        port: %s%n"
                        + "        maxConcurrentRequests: %d%n"
                        + "        jmx: %b%n"
                        + "SSL configuration:%n%s%n"
                        + "Authentication configuration:%n%s%n"
                        + "datastax-java-driver configuration: %n%s",
                getKeyspaceName(),
                getTableName(),
                getColumnsRegexp(),
                getEventsTopic(),
                getEventsSubscriptionName(),
                getEventsSubscriptionType(),
                getBatchSize(),
                getQueryExecutors(),
                getQueryMinMobileAvgLatency(),
                getQueryMaxMobileAvgLatency(),
                getQueryBackoffInMs(),
                getQueryMaxBackoffInSec(),
                getCacheMaxDigests(),
                getCachePersistentDirectory(),
                getCacheMaxCapacity(),
                getCacheExpireAfterMs(),
                getCacheOnlyIfCoordinatorMatch(),
                getContactPoints(),
                getPortToString(),
                getMaxConcurrentRequests(),
                getJmx(),
                getSslConfigToString(),
                Splitter.on("\n")
                        .splitToList(authConfig.toString())
                        .stream()
                        .map(line -> "        " + line)
                        .collect(Collectors.joining("\n")),
                javaDriverSettings
                        .entrySet()
                        .stream()
                        .map(entry -> "      " + entry)
                        .collect(Collectors.joining("\n")));
    }

    private String getSslConfigToString() {
        if (sslConfig != null) {
            return Splitter.on("\n")
                    .splitToList(sslConfig.toString())
                    .stream()
                    .map(line -> "        " + line)
                    .collect(Collectors.joining("\n"));
        } else {
            return "SslConfig not present";
        }
    }

    private String getPortToString() {
        if (isCloud()) {
            return String.format("%s will be ignored because you are using cloud", PORT_OPT);
        }
        return String.valueOf(getPort());
    }

    public Map<String, String> getJavaDriverSettings() {
        return javaDriverSettings;
    }
}
