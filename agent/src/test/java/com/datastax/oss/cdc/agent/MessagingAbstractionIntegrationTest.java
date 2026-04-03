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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.messaging.config.*;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for AbstractMessagingMutationSender with messaging abstraction layer.
 * Tests verify that the agent module correctly uses the messaging abstraction to send mutations.
 *
 * Note: These tests focus on configuration mapping and do not require actual Pulsar/Kafka connections.
 */
@DisplayName("Messaging Abstraction Integration Tests for Agent")
public class MessagingAbstractionIntegrationTest {

    private TestMutationSender mutationSender;
    private AgentConfig config;

    /**
     * Test implementation of AbstractMessagingMutationSender for testing purposes.
     */
    private static class TestMutationSender extends AbstractMessagingMutationSender<String> {
        private int skippedMutations = 0;
        private final UUID hostId = UUID.randomUUID();

        public TestMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
            super(config, useMurmur3Partitioner);
        }

        @Override
        public Schema getNativeSchema(String cql3Type) {
            return Schema.create(Schema.Type.STRING);
        }

        @Override
        public Object cqlToAvro(String metadata, String columnName, Object value) {
            return value != null ? value.toString() : null;
        }

        @Override
        public boolean isSupported(AbstractMutation<String> mutation) {
            return true;
        }

        @Override
        public void incSkippedMutations() {
            skippedMutations++;
        }

        @Override
        public UUID getHostId() {
            return hostId;
        }
    }

    /**
     * Test implementation of ColumnInfo interface.
     */
    private static class TestColumnInfo implements ColumnInfo {
        private final String name;
        private final String cql3Type;
        private final boolean isClusteringKey;

        public TestColumnInfo(String name, String cql3Type, boolean isClusteringKey) {
            this.name = name;
            this.cql3Type = cql3Type;
            this.isClusteringKey = isClusteringKey;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String cql3Type() {
            return cql3Type;
        }

        @Override
        public boolean isClusteringKey() {
            return isClusteringKey;
        }
    }

    /**
     * Test implementation of TableInfo interface.
     */
    private static class TestTableInfo implements TableInfo {
        private final String keyspace;
        private final String name;
        private final List<ColumnInfo> primaryKeyColumns;

        public TestTableInfo(String keyspace, String name, List<ColumnInfo> primaryKeyColumns) {
            this.keyspace = keyspace;
            this.name = name;
            this.primaryKeyColumns = primaryKeyColumns;
        }

        @Override
        public String key() {
            return keyspace + "." + name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String keyspace() {
            return keyspace;
        }

        @Override
        public List<ColumnInfo> primaryKeyColumns() {
            return primaryKeyColumns;
        }
    }

    @BeforeEach
    void setUp() {
        config = new AgentConfig();
        config.pulsarServiceUrl = "pulsar://localhost:6650";
        config.topicPrefix = "test-events-";
        config.pulsarBatchDelayInMs = 10;
        config.pulsarMaxPendingMessages = 1000;
        config.pulsarKeyBasedBatcher = false;
    }

    @Test
    @DisplayName("Test configuration mapping from AgentConfig to ClientConfig")
    void testConfigurationMapping() {
        config.pulsarBatchDelayInMs = 100;
        config.pulsarMaxPendingMessages = 500;
        config.pulsarKeyBasedBatcher = true;
        config.pulsarMemoryLimitBytes = 1024L;
        
        mutationSender = new TestMutationSender(config, true);
        ClientConfig clientConfig = mutationSender.buildClientConfig(config);
        
        assertNotNull(clientConfig, "Client config should not be null");
        assertEquals(MessagingProvider.PULSAR, clientConfig.getProvider(),
                "Provider should be PULSAR");
        assertEquals(config.pulsarServiceUrl, clientConfig.getServiceUrl(),
                "Service URL should match");
        assertEquals(config.pulsarMemoryLimitBytes, clientConfig.getMemoryLimitBytes(),
                "Memory limit should match");
    }

    @Test
    @DisplayName("Test batch configuration mapping")
    void testBatchConfigurationMapping() {
        config.pulsarBatchDelayInMs = 50;
        config.pulsarKeyBasedBatcher = true;
        
        mutationSender = new TestMutationSender(config, false);
        BatchConfig batchConfig = mutationSender.buildBatchConfig(config);
        
        assertNotNull(batchConfig, "Batch config should not be null");
        assertTrue(batchConfig.isEnabled(), "Batching should be enabled");
        assertEquals(50, batchConfig.getMaxDelayMs(), "Max delay should match");
        assertTrue(batchConfig.isKeyBasedBatching(), "Key-based batching should be enabled");
    }

    @Test
    @DisplayName("Test batch configuration disabled when delay is zero")
    void testBatchConfigurationDisabled() {
        config.pulsarBatchDelayInMs = 0;
        
        mutationSender = new TestMutationSender(config, false);
        BatchConfig batchConfig = mutationSender.buildBatchConfig(config);
        
        assertNotNull(batchConfig, "Batch config should not be null");
        assertFalse(batchConfig.isEnabled(), "Batching should be disabled when delay is 0");
    }

    @Test
    @DisplayName("Test routing configuration with Murmur3 partitioner")
    void testRoutingConfigurationWithMurmur3() {
        mutationSender = new TestMutationSender(config, true);
        RoutingConfig routingConfig = mutationSender.buildRoutingConfig(config, true);
        
        assertNotNull(routingConfig, "Routing config should not be null when Murmur3 is enabled");
        assertEquals(RoutingConfig.RoutingMode.CUSTOM, routingConfig.getRoutingMode(),
                "Routing mode should be CUSTOM");
        assertNotNull(routingConfig.getCustomRouterClassName(),
                "Custom router class name should be set");
    }

    @Test
    @DisplayName("Test routing configuration without Murmur3 partitioner")
    void testRoutingConfigurationWithoutMurmur3() {
        mutationSender = new TestMutationSender(config, false);
        RoutingConfig routingConfig = mutationSender.buildRoutingConfig(config, false);
        
        assertNull(routingConfig, "Routing config should be null when Murmur3 is disabled");
    }

    @Test
    @DisplayName("Test SSL configuration mapping")
    void testSslConfigurationMapping() {
        config.tlsTrustCertsFilePath = "/path/to/truststore.pem";
        config.sslHostnameVerificationEnable = true;
        config.useKeyStoreTls = true;
        config.sslKeystorePath = "/path/to/keystore.jks";
        config.sslTruststorePassword = "password";
        config.sslTruststoreType = "JKS";
        config.sslCipherSuites = "TLS_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA";
        config.sslEnabledProtocols = "TLSv1.2,TLSv1.3";
        
        mutationSender = new TestMutationSender(config, false);
        SslConfig sslConfig = mutationSender.buildSslConfig(config);
        
        assertNotNull(sslConfig, "SSL config should not be null");
        
        // Verify Optional values are present and match
        assertTrue(sslConfig.getTrustedCertificates().isPresent(),
                "Trusted certificates should be present");
        assertEquals(config.tlsTrustCertsFilePath, sslConfig.getTrustedCertificates().get(),
                "Trusted certificates path should match");
        
        assertTrue(sslConfig.isHostnameVerificationEnabled(),
                "Hostname verification should be enabled");
        
        assertTrue(sslConfig.getKeyStorePath().isPresent(),
                "Keystore path should be present");
        assertEquals(config.sslKeystorePath, sslConfig.getKeyStorePath().get(),
                "Keystore path should match");
        
        assertTrue(sslConfig.getCipherSuites().isPresent(), "Cipher suites should be present");
        assertEquals(2, sslConfig.getCipherSuites().get().size(),
                "Should have 2 cipher suites");
        
        assertTrue(sslConfig.getProtocols().isPresent(), "Protocols should be present");
        assertEquals(2, sslConfig.getProtocols().get().size(),
                "Should have 2 protocols");
    }

    @Test
    @DisplayName("Test authentication configuration mapping")
    void testAuthConfigurationMapping() {
        config.pulsarAuthPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";
        config.pulsarAuthParams = "token:eyJhbGciOiJIUzI1NiJ9";
        
        mutationSender = new TestMutationSender(config, false);
        AuthConfig authConfig = mutationSender.buildAuthConfig(config);
        
        assertNotNull(authConfig, "Auth config should not be null");
        assertEquals(config.pulsarAuthPluginClassName, authConfig.getPluginClassName(),
                "Auth plugin class name should match");
        assertEquals(config.pulsarAuthParams, authConfig.getAuthParams(),
                "Auth params should match");
    }

    @Test
    @DisplayName("Test provider determination defaults to Pulsar")
    void testProviderDeterminationDefaultsPulsar() {
        mutationSender = new TestMutationSender(config, false);
        MessagingProvider provider = mutationSender.determineProvider(config);
        
        assertEquals(MessagingProvider.PULSAR, provider,
                "Should default to PULSAR when not specified");
    }

    @Test
    @DisplayName("Test provider determination with explicit Kafka")
    void testProviderDeterminationKafka() {
        config.messagingProvider = "KAFKA";
        config.kafkaBootstrapServers = "localhost:9092";
        
        mutationSender = new TestMutationSender(config, false);
        MessagingProvider provider = mutationSender.determineProvider(config);
        
        assertEquals(MessagingProvider.KAFKA, provider,
                "Should use KAFKA when explicitly specified");
    }

    @Test
    @DisplayName("Test Kafka configuration mapping")
    void testKafkaConfigurationMapping() {
        config.messagingProvider = "KAFKA";
        config.kafkaBootstrapServers = "localhost:9092";
        config.kafkaAcks = "all";
        config.kafkaCompressionType = "snappy";
        config.kafkaBatchSize = 16384;
        config.kafkaLingerMs = 10;
        config.kafkaMaxInFlightRequests = 5;
        config.kafkaSchemaRegistryUrl = "http://localhost:8081";
        
        mutationSender = new TestMutationSender(config, false);
        ClientConfig clientConfig = mutationSender.buildClientConfig(config);
        
        assertNotNull(clientConfig, "Client config should not be null");
        assertEquals(MessagingProvider.KAFKA, clientConfig.getProvider(),
                "Provider should be KAFKA");
        assertEquals(config.kafkaBootstrapServers, clientConfig.getServiceUrl(),
                "Bootstrap servers should match");
        
        Map<String, Object> providerProps = clientConfig.getProviderProperties();
        assertNotNull(providerProps, "Provider properties should not be null");
        assertTrue(providerProps.containsKey("acks"), "Should contain acks property");
        assertTrue(providerProps.containsKey("compression.type"), "Should contain compression.type property");
        assertTrue(providerProps.containsKey("batch.size"), "Should contain batch.size property");
        assertTrue(providerProps.containsKey("linger.ms"), "Should contain linger.ms property");
        assertTrue(providerProps.containsKey("max.in.flight.requests.per.connection"),
                "Should contain max.in.flight.requests.per.connection property");
        assertTrue(providerProps.containsKey("schema.registry.url"),
                "Should contain schema.registry.url property");
    }

    @Test
    @DisplayName("Test AVRO key schema generation")
    void testAvroKeySchemaGeneration() {
        mutationSender = new TestMutationSender(config, false);
        
        ColumnInfo col1 = new TestColumnInfo("id", "text", false);
        ColumnInfo col2 = new TestColumnInfo("timestamp", "timestamp", true);
        TableInfo tableInfo = new TestTableInfo("test_keyspace", "test_table",
                Arrays.asList(col1, col2));
        
        AbstractMessagingMutationSender.SchemaAndWriter schemaAndWriter =
                mutationSender.getAvroKeySchema(tableInfo);
        
        assertNotNull(schemaAndWriter, "Schema and writer should not be null");
        assertNotNull(schemaAndWriter.schema, "AVRO schema should not be null");
        assertNotNull(schemaAndWriter.writer, "Datum writer should not be null");
        assertEquals("test_table", schemaAndWriter.schema.getName(),
                "Schema name should match table name");
        assertEquals(2, schemaAndWriter.schema.getFields().size(),
                "Schema should have 2 fields");
    }

    @Test
    @DisplayName("Test schema caching")
    void testSchemaCaching() {
        mutationSender = new TestMutationSender(config, false);
        
        ColumnInfo col = new TestColumnInfo("id", "text", false);
        TableInfo tableInfo = new TestTableInfo("test_keyspace", "test_table",
                Collections.singletonList(col));
        
        AbstractMessagingMutationSender.SchemaAndWriter schema1 =
                mutationSender.getAvroKeySchema(tableInfo);
        AbstractMessagingMutationSender.SchemaAndWriter schema2 =
                mutationSender.getAvroKeySchema(tableInfo);
        
        assertSame(schema1, schema2, "Schema should be cached and return same instance");
    }

    @Test
    @DisplayName("Test resource cleanup on close")
    void testResourceCleanup() {
        mutationSender = new TestMutationSender(config, false);
        
        // Close should not throw even without initialization
        assertDoesNotThrow(() -> mutationSender.close(),
                "Close should not throw exception");
    }

    @Test
    @DisplayName("Test multiple close calls are safe")
    void testMultipleCloseCallsAreSafe() {
        mutationSender = new TestMutationSender(config, false);
        
        assertDoesNotThrow(() -> {
            mutationSender.close();
            mutationSender.close();
            mutationSender.close();
        }, "Multiple close calls should be safe");
    }
}