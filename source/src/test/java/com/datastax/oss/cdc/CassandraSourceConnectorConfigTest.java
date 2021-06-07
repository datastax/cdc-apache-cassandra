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
import com.datastax.oss.common.sink.util.SinkUtil;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.dsbulk.tests.assertions.TestAssertions;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.datastax.oss.cdc.CassandraSourceConnectorConfig.*;
import static com.datastax.oss.common.sink.config.AuthenticatorConfig.KEYTAB_OPT;
import static com.datastax.oss.common.sink.config.SslConfig.*;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(LogInterceptingExtension.class)
class CassandraSourceConnectorConfigTest {

    private static final String CONTACT_POINTS_DRIVER_SETTINGS = withDriverPrefix(CONTACT_POINTS);

    @Test
    void should_error_invalid_port() {
        Map<String, String> props =
                Maps.newHashMap(ImmutableMap.<String, String>builder().put(PORT_OPT, "foo").build());
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value foo for configuration port");

        props.put(PORT_OPT, "0");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");

        props.put(PORT_OPT, "-1");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");
    }

    @Test
    void should_error_invalid_queryExecutionTimeout() {
        Map<String, String> props =
                Maps.newHashMap(
                        ImmutableMap.<String, String>builder().put(QUERY_EXECUTION_TIMEOUT_OPT, "foo").build());
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value foo for configuration queryExecutionTimeout");

        props.put(QUERY_EXECUTION_TIMEOUT_OPT, "0");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");

        props.put(QUERY_EXECUTION_TIMEOUT_OPT, "-1");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");
    }

    @Test
    void should_error_invalid_metricsHighestLatency() {
        Map<String, String> props =
                Maps.newHashMap(
                        ImmutableMap.<String, String>builder().put(METRICS_HIGHEST_LATENCY_OPT, "foo").build());
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value foo for configuration metricsHighestLatency");

        props.put(METRICS_HIGHEST_LATENCY_OPT, "0");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");

        props.put(METRICS_HIGHEST_LATENCY_OPT, "-1");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");
    }

    @Test
    void should_error_invalid_connectionPoolLocalSize() {
        Map<String, String> props =
                Maps.newHashMap(
                        ImmutableMap.<String, String>builder().put(CassandraSourceConnectorConfig.CONNECTION_POOL_LOCAL_SIZE, "foo").build());
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value foo for configuration connectionPoolLocalSize");

        props.put(CassandraSourceConnectorConfig.CONNECTION_POOL_LOCAL_SIZE, "0");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");

        props.put(CassandraSourceConnectorConfig.CONNECTION_POOL_LOCAL_SIZE, "-1");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");
    }

    @Test
    void should_error_invalid_maxConcurrentRequests() {
        Map<String, String> props =
                Maps.newHashMap(
                        ImmutableMap.<String, String>builder().put(CONCURRENT_REQUESTS_OPT, "foo").build());
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value foo for configuration maxConcurrentRequests");

        props.put(CONCURRENT_REQUESTS_OPT, "0");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");

        props.put(CONCURRENT_REQUESTS_OPT, "-1");
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Value must be at least 1");
    }

    @Test
    void should_error_invalid_compression_type() {
        Map<String, String> props =
                Maps.newHashMap(ImmutableMap.<String, String>builder().put(COMPRESSION_OPT, "foo").build());
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format(
                                "Invalid value foo for configuration %s: valid values are none, snappy, lz4",
                                COMPRESSION_OPT));
    }

    @Test
    void should_error_missing_dc_with_contactPoints() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder().put(CONTACT_POINTS_OPT, "127.0.0.1").build();
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format("When contact points is provided, %s must also be specified", DC_OPT));
    }

    @Test
    void should_error_empty_dc_with_contactPoints() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(CONTACT_POINTS_OPT, "127.0.0.1")
                        .put(DC_OPT, "")
                        .build();
        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format("When contact points is provided, %s must also be specified", DC_OPT));
    }

    @Test
    void should_handle_dc_with_contactPoints() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(CONTACT_POINTS_OPT, "127.0.0.1, 127.0.1.1")
                        .put(DC_OPT, "local")
                        .build();

        CassandraSourceConnectorConfig d = new CassandraSourceConnectorConfig(props);
        assertThat(d.getContactPoints()).containsExactly("127.0.0.1", "127.0.1.1");
        assertThat(d.getLocalDc().get()).isEqualTo("local");
    }

    @Test
    void should_handle_dc_with_contactPoints_driver_prefix() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(CONTACT_POINTS_OPT, "127.0.0.1, 127.0.1.1")
                        .put(LOCAL_DC_DRIVER_SETTING, "local")
                        .build();

        CassandraSourceConnectorConfig d = new CassandraSourceConnectorConfig(props);
        assertThat(d.getContactPoints()).containsExactly("127.0.0.1", "127.0.1.1");
        assertThat(d.getLocalDc().get()).isEqualTo("local");
    }

    @Test
    void should_handle_port() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder().put(PORT_OPT, "5725").build();

        CassandraSourceConnectorConfig d = new CassandraSourceConnectorConfig(props);
        assertThat(d.getPort()).isEqualTo(5725);
    }

    @Test
    void should_handle_maxConcurrentRequests() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder().put(CONCURRENT_REQUESTS_OPT, "129").build();

        CassandraSourceConnectorConfig d = new CassandraSourceConnectorConfig(props);
        assertThat(d.getMaxConcurrentRequests()).isEqualTo(129);
    }

    @Test
    void should_handle_instance_name() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(KEYSPACE_NAME_CONFIG, "ks1")
                        .put(TABLE_NAME_CONFIG, "table1")
                        .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                        .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                        .put(SinkUtil.NAME_OPT, "myinst")
                        .build();

        CassandraSourceConnectorConfig d = new CassandraSourceConnectorConfig(props);
        assertThat(d.getInstanceName()).isEqualTo("myinst");
    }

    @Test
    void should_handle_secure_connect_bundle() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(KEYSPACE_NAME_CONFIG, "ks1")
                        .put(TABLE_NAME_CONFIG, "table1")
                        .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                        .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                        .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
                        .build();

        CassandraSourceConnectorConfig d = new CassandraSourceConnectorConfig(props);
        assertThat(d.getJavaDriverSettings().get(SECURE_CONNECT_BUNDLE_DRIVER_SETTING))
                .isEqualTo("/location/to/bundle");
    }

    @Test
    void should_throw_when_secure_connect_bundle_and_contact_points_provided() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(KEYSPACE_NAME_CONFIG, "ks1")
                        .put(TABLE_NAME_CONFIG, "table1")
                        .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                        .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                        .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
                        .put(CONTACT_POINTS_OPT, "127.0.0.1")
                        .build();

        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format(
                                "When %s parameter is specified you should not provide %s.",
                                SECURE_CONNECT_BUNDLE_OPT, CONTACT_POINTS_OPT));
    }

    @Test
    void should_throw_when_secure_connect_bundle_and_local_dc_provided() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(KEYSPACE_NAME_CONFIG, "ks1")
                        .put(TABLE_NAME_CONFIG, "table1")
                        .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                        .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                        .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
                        .put(DC_OPT, "dc1")
                        .build();

        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format(
                                "When %s parameter is specified you should not provide %s.",
                                SECURE_CONNECT_BUNDLE_OPT, DC_OPT));
    }

    @Test
    void should_throw_when_secure_connect_bundle_and_local_dc_driver_setting_provided() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(KEYSPACE_NAME_CONFIG, "ks1")
                        .put(TABLE_NAME_CONFIG, "table1")
                        .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                        .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                        .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
                        .put(LOCAL_DC_DRIVER_SETTING, "dc1")
                        .build();

        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format(
                                "When %s parameter is specified you should not provide %s.",
                                SECURE_CONNECT_BUNDLE_OPT, DC_OPT));
    }

    @Test
    void should_throw_when_secure_connect_bundle_and_ssl_setting_provided() {
        Map<String, String> props =
                ImmutableMap.<String, String>builder()
                        .put(KEYSPACE_NAME_CONFIG, "ks1")
                        .put(TABLE_NAME_CONFIG, "table1")
                        .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                        .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                        .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
                        .put(PROVIDER_OPT, "JDK")
                        .build();

        assertThatThrownBy(() -> new CassandraSourceConnectorConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(
                        String.format(
                                "When %s parameter is specified you should not provide any setting under %s.",
                                SECURE_CONNECT_BUNDLE_OPT, SSL_OPT_PREFIX));
    }

    @Test
    void should_fill_with_default_metrics_settings_if_jmx_enabled() {
        // given
        Map<String, String> props =
                Maps.newHashMap(ImmutableMap.<String, String>builder().put("jmx", "true").build());
        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(props);

        // then
        assertThat(
                CassandraSourceConnectorConfig
                        .getJavaDriverSettings()
                        .get(withDriverPrefix(METRICS_SESSION_ENABLED) + ".0"))
                .isEqualTo("cql-requests");
        assertThat(
                CassandraSourceConnectorConfig
                        .getJavaDriverSettings()
                        .get(withDriverPrefix(METRICS_SESSION_ENABLED) + ".1"))
                .isEqualTo("cql-client-timeouts");
        assertThat(
                CassandraSourceConnectorConfig
                        .getJavaDriverSettings()
                        .get(withDriverPrefix(METRICS_SESSION_CQL_REQUESTS_INTERVAL)))
                .isEqualTo(METRICS_INTERVAL_DEFAULT);
    }

    @Test
    void should_override_default_metrics_settings_if_jmx_and_metrics_settings_are_provided_enabled() {
        // given
        Map<String, String> props =
                Maps.newHashMap(
                        ImmutableMap.<String, String>builder()
                                .put(KEYSPACE_NAME_CONFIG, "ks1")
                                .put(TABLE_NAME_CONFIG, "table1")
                                .put(EVENTS_TOPIC_NAME_CONFIG, "events-ks1.table1")
                                .put(DATA_TOPIC_NAME_CONFIG, "data-ks1.table1")
                                .put("jmx", "true")
                                .put(withDriverPrefix(METRICS_SESSION_ENABLED) + ".0", "bytes-sent")
                                .put(withDriverPrefix(METRICS_SESSION_CQL_REQUESTS_INTERVAL), "5 seconds")
                                .build());
        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(props);

        // then
        assertThat(
                CassandraSourceConnectorConfig
                        .getJavaDriverSettings()
                        .get(withDriverPrefix(METRICS_SESSION_ENABLED) + ".0"))
                .isEqualTo("bytes-sent");
        assertThat(
                CassandraSourceConnectorConfig
                        .getJavaDriverSettings()
                        .get(withDriverPrefix(METRICS_SESSION_CQL_REQUESTS_INTERVAL)))
                .isEqualTo("5 seconds");
    }

    @ParameterizedTest
    @MethodSource("deprecatedSettingsProvider")
    void should_handle_deprecated_settings(
            Map<String, String> inputSettings, String driverSettingName, String expected) {
        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(inputSettings);

        // then
        assertThat(CassandraSourceConnectorConfig.getJavaDriverSettings().get(driverSettingName))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("javaDriverSettingProvider")
    void should_use_java_driver_setting(
            Map<String, String> inputSettings, String driverSettingName, String expected) {
        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(inputSettings);

        // then
        assertThat(CassandraSourceConnectorConfig.getJavaDriverSettings().get(driverSettingName))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("defaultSettingProvider")
    void should_set_default_driver_setting(String driverSettingName, String expectedDefault) {
        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(Collections.emptyMap());

        // then
        assertThat(CassandraSourceConnectorConfig.getJavaDriverSettings().get(driverSettingName))
                .isEqualTo(expectedDefault);
    }

    @Test
    void should_unpack_base64_secureBundle_legacy_name() throws Exception {
        should_unpack_base64_file(
                SECURE_CONNECT_BUNDLE_OPT,
                (CassandraSourceConnectorConfig config) -> {
                    assertThat(config.isCloud()).isEqualTo(true);
                    return config.getJavaDriverSettings().get(SECURE_CONNECT_BUNDLE_DRIVER_SETTING);
                });
    }

    @Test
    void should_unpack_base64_secureBundle() throws Exception {
        should_unpack_base64_file(
                SECURE_CONNECT_BUNDLE_DRIVER_SETTING,
                (CassandraSourceConnectorConfig config) -> {
                    assertThat(config.isCloud()).isEqualTo(true);
                    return config.getJavaDriverSettings().get(SECURE_CONNECT_BUNDLE_DRIVER_SETTING);
                });
    }

    @Test
    void should_unpack_base64_ssl_keystore() throws Exception {
        should_unpack_base64_file(
                KEYSTORE_PATH_OPT,
                (CassandraSourceConnectorConfig config) -> {
                    return config.getSslConfig().getKeystorePath().toString();
                });
    }

    @Test
    void should_unpack_base64_ssl_trustore() throws Exception {
        should_unpack_base64_file(
                TRUSTSTORE_PATH_OPT,
                (CassandraSourceConnectorConfig config) -> {
                    return config.getSslConfig().getTruststorePath().toString();
                });
    }

    @Test
    void should_unpack_base64_ssl_openssl_private_key() throws Exception {
        should_unpack_base64_file(
                OPENSSL_PRIVATE_KEY_OPT,
                (CassandraSourceConnectorConfig config) -> {
                    return config.getSslConfig().getOpenSslPrivateKey().toString();
                });
    }

    @Test
    void should_unpack_base64_ssl_openssl_cert_chain() throws Exception {
        should_unpack_base64_file(
                OPENSSL_KEY_CERT_CHAIN_OPT,
                (CassandraSourceConnectorConfig config) -> {
                    return config.getSslConfig().getOpenSslKeyCertChain().toString();
                });
    }

    @Test
    void should_unpack_base64_auth_keytab() throws Exception {
        should_unpack_base64_file(
                KEYTAB_OPT,
                (CassandraSourceConnectorConfig config) -> {
                    return config.getAuthenticatorConfig().getKeyTabPath().toString();
                });
    }

    void should_unpack_base64_file(
            String entryName, Function<CassandraSourceConnectorConfig, String> parameterAccessor) throws Exception {

        byte[] zipFileContents = "foo".getBytes(UTF_8);
        String encoded = "base64:" + Base64.getEncoder().encodeToString(zipFileContents);
        Map<String, String> inputSettings = new HashMap<>();
        inputSettings.put(entryName, encoded);
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(inputSettings);

        String path = parameterAccessor.apply(CassandraSourceConnectorConfig);
        File file = new File(path);
        assertThat(file.isFile()).isEqualTo(true);
        Set<PosixFilePermission> posixFilePermissions = Files.getPosixFilePermissions(file.toPath());
        assertThat(posixFilePermissions)
                .contains(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
        assertThat(posixFilePermissions.size())
                .as("bad permissions " + posixFilePermissions)
                .isEqualTo(2);
        byte[] content = Files.readAllBytes(file.toPath());
        assertThat(content).isEqualTo(zipFileContents);
    }

    @Test
    void should_transform_list_setting_to_indexed_typesafe_setting() {
        // given
        Map<String, String> connectorSettings = new HashMap<>();
        for (String listSettingName : JAVA_DRIVER_SETTINGS_LIST_TYPE) {
            connectorSettings.put(listSettingName, "a,b");
        }
        // when contact-points are provided the dc must also be provided
        connectorSettings.put(DC_OPT, "dc");

        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(connectorSettings);

        // then
        for (String listSettingName : JAVA_DRIVER_SETTINGS_LIST_TYPE) {
            assertThat(
                    CassandraSourceConnectorConfig
                            .getJavaDriverSettings()
                            .get(String.format("%s.%s", listSettingName, "0")))
                    .isEqualTo("a");
            assertThat(
                    CassandraSourceConnectorConfig
                            .getJavaDriverSettings()
                            .get(String.format("%s.%s", listSettingName, "1")))
                    .isEqualTo("b");
        }
    }

    @ParameterizedTest
    @MethodSource("contactPointsProvider")
    void should_handle_contact_points_provided_using_connector_and_driver_prefix(
            String connectorContactPoints,
            String javaDriverPrefixContactPoints,
            List<String> expected,
            Map<String, String> expectedDriverSettings) {
        // given
        Map<String, String> connectorSettings = new HashMap<>();
        if (connectorContactPoints != null) {
            connectorSettings.put(CONTACT_POINTS_OPT, connectorContactPoints);
        }
        if (javaDriverPrefixContactPoints != null) {
            connectorSettings.put(CONTACT_POINTS_DRIVER_SETTINGS, javaDriverPrefixContactPoints);
        }
        connectorSettings.put(LOCAL_DC_DRIVER_SETTING, "localDc");

        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(connectorSettings);

        // then
        assertThat(CassandraSourceConnectorConfig.getContactPoints()).isEqualTo(expected);
        for (Map.Entry<String, String> entry : expectedDriverSettings.entrySet()) {
            assertThat(CassandraSourceConnectorConfig.getJavaDriverSettings()).contains(entry);
        }
    }

    private static Stream<? extends Arguments> sourceProvider() {
        return Stream.of(
                Arguments.of("keyspace1", "table1", "events-keyspace1.table1", "data-keyspace1.table1")
        );
    }

    private static Stream<? extends Arguments> contactPointsProvider() {
        return Stream.of(
                Arguments.of("a, b", null, ImmutableList.of("a", "b"), Collections.emptyMap()),
                Arguments.of("a, b", "c", ImmutableList.of("a", "b"), Collections.emptyMap()),
                Arguments.of(
                        null,
                        " c, d",
                        Collections.emptyList(), // setting provided with datastax-java-driver
                        // prefix should not be returned by the getContactPoints() method
                        ImmutableMap.of(
                                CONTACT_POINTS_DRIVER_SETTINGS + ".0",
                                "c",
                                CONTACT_POINTS_DRIVER_SETTINGS + ".1",
                                "d")) // pass cp setting to the driver directly
        );
    }

    private static Stream<? extends Arguments> defaultSettingProvider() {
        return Stream.of(
                Arguments.of(QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING, QUERY_EXECUTION_TIMEOUT_DEFAULT),
                Arguments.of(METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS, METRICS_HIGHEST_LATENCY_DEFAULT),
                Arguments.of(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING, CONNECTION_POOL_LOCAL_SIZE_DEFAULT),
                Arguments.of(LOCAL_DC_DRIVER_SETTING, null),
                Arguments.of(COMPRESSION_DRIVER_SETTING, COMPRESSION_DEFAULT),
                Arguments.of(SECURE_CONNECT_BUNDLE_DRIVER_SETTING, null));
    }

    private static Stream<? extends Arguments> deprecatedSettingsProvider() {
        return Stream.of(
                Arguments.of(
                        ImmutableMap.of(
                                QUERY_EXECUTION_TIMEOUT_OPT, "10", QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING, "100"),
                        QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING,
                        "10 seconds"),
                Arguments.of(
                        ImmutableMap.of(QUERY_EXECUTION_TIMEOUT_OPT, "10"),
                        QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING,
                        "10 seconds"),
                Arguments.of(
                        ImmutableMap.of(
                                METRICS_HIGHEST_LATENCY_OPT, "10", METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS, "100"),
                        METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS,
                        "10 seconds"),
                Arguments.of(
                        ImmutableMap.of(METRICS_HIGHEST_LATENCY_OPT, "10"),
                        METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS,
                        "10 seconds"),
                Arguments.of(
                        ImmutableMap.of(
                                CassandraSourceConnectorConfig.CONNECTION_POOL_LOCAL_SIZE, "10", CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING, "100"),
                        CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING,
                        "10"),
                Arguments.of(
                        ImmutableMap.of(CassandraSourceConnectorConfig.CONNECTION_POOL_LOCAL_SIZE, "10"),
                        CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING,
                        "10"),
                Arguments.of(
                        ImmutableMap.of(DC_OPT, "dc", LOCAL_DC_DRIVER_SETTING, "dc_suppressed"),
                        LOCAL_DC_DRIVER_SETTING,
                        "dc"),
                Arguments.of(ImmutableMap.of(DC_OPT, "dc"), LOCAL_DC_DRIVER_SETTING, "dc"),
                Arguments.of(
                        ImmutableMap.of(COMPRESSION_OPT, "lz4", COMPRESSION_DRIVER_SETTING, "none"),
                        COMPRESSION_DRIVER_SETTING,
                        "lz4"),
                Arguments.of(ImmutableMap.of(COMPRESSION_OPT, "lz4"), COMPRESSION_DRIVER_SETTING, "lz4"),
                Arguments.of(
                        ImmutableMap.of(
                                SECURE_CONNECT_BUNDLE_OPT, "path", SECURE_CONNECT_BUNDLE_DRIVER_SETTING, "path2"),
                        SECURE_CONNECT_BUNDLE_DRIVER_SETTING,
                        "path"),
                Arguments.of(
                        ImmutableMap.of(SECURE_CONNECT_BUNDLE_OPT, "path"),
                        SECURE_CONNECT_BUNDLE_DRIVER_SETTING,
                        "path"));
    }

    private static Stream<? extends Arguments> javaDriverSettingProvider() {
        return Stream.of(
                Arguments.of(
                        ImmutableMap.of(QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING, "100 seconds"),
                        QUERY_EXECUTION_TIMEOUT_DRIVER_SETTING,
                        "100 seconds"),
                Arguments.of(
                        ImmutableMap.of(METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS, "100 seconds"),
                        METRICS_HIGHEST_LATENCY_DRIVER_SETTINGS,
                        "100 seconds"),
                Arguments.of(
                        ImmutableMap.of(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING, "100"),
                        CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING,
                        "100"),
                Arguments.of(ImmutableMap.of(LOCAL_DC_DRIVER_SETTING, "dc"), LOCAL_DC_DRIVER_SETTING, "dc"),
                Arguments.of(
                        ImmutableMap.of(COMPRESSION_DRIVER_SETTING, "lz4"), COMPRESSION_DRIVER_SETTING, "lz4"),
                Arguments.of(
                        ImmutableMap.of(SECURE_CONNECT_BUNDLE_DRIVER_SETTING, "path"),
                        SECURE_CONNECT_BUNDLE_DRIVER_SETTING,
                        "path"));
    }

    @ParameterizedTest
    @MethodSource
    void should_handle_ignore_errors(
            String ignoreErrors,
            IgnoreErrorsPolicy expected,
            String expectedDeprecationLogWarning,
            LogInterceptor logs) {
        // given
        Map<String, String> connectorSettings = new HashMap<>();
        connectorSettings.put(IGNORE_ERRORS, ignoreErrors);

        // when
        CassandraSourceConnectorConfig CassandraSourceConnectorConfig = new CassandraSourceConnectorConfig(connectorSettings);

        // then
        assertThat(CassandraSourceConnectorConfig.getIgnoreErrors()).isEqualTo(expected);
        if (expectedDeprecationLogWarning != null) {
            TestAssertions.assertThat(logs).hasMessageContaining(expectedDeprecationLogWarning);
        }
    }

    private static Stream<Arguments> should_handle_ignore_errors() {
        return Stream.of(
                Arguments.of("ALL", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.ALL, null),
                Arguments.of("all", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.ALL, null),
                Arguments.of("All", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.ALL, null),
                Arguments.of("NONE", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.NONE, null),
                Arguments.of("none", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.NONE, null),
                Arguments.of("None", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.NONE, null),
                Arguments.of("DRIVER", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.DRIVER, null),
                Arguments.of("driver", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.DRIVER, null),
                Arguments.of("Driver", CassandraSourceConnectorConfig.IgnoreErrorsPolicy.DRIVER, null),
                // deprecated settings
                Arguments.of(
                        "true",
                        IgnoreErrorsPolicy.DRIVER,
                        "Setting ignoreErrors=true is deprecated, please replace with ignoreErrors=Driver"),
                Arguments.of(
                        "false",
                        IgnoreErrorsPolicy.NONE,
                        "Setting ignoreErrors=false is deprecated, please replace with ignoreErrors=None"));
    }
}
