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

import com.datastax.oss.cdc.AgentTestUtil;
import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.cdc.ConverterAndQuery;
import com.datastax.oss.cdc.converters.AvroRowConverter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs KafkaCassandraSourceTask directly (bypassing the Kafka Connect worker/REST layer)
 * against a real Kafka broker and a real Cassandra node with the actual CDC agent attached,
 * proving the end-to-end read path: agent publishes a dirty event, the task deduplicates and
 * reads the row back from Cassandra, and emits a SourceRecord with the real row content.
 */
@Slf4j
public class KafkaCassandraSourceTaskContainerTests {

    private static final String CONTAINER_KAFKA_CONFIG_PATH = "/etc/cassandra/cdc-kafka.conf";

    private static Network testNetwork;
    // cp-kafka uses the legacy KafkaContainer (org.testcontainers.containers) whose startup
    // script calls /etc/confluent/docker/run. The new KafkaContainer (org.testcontainers.kafka)
    // calls /etc/kafka/docker/run, which only exists in apache/kafka images.
    private static org.testcontainers.containers.KafkaContainer confluentKafkaContainer;
    private static KafkaContainer ossKafkaContainer;
    private static CassandraContainer<?> cassandraContainer;
    private static ConfluentSchemaRegistryContainer confluentSchemaRegistryContainer;
    private static ApicurioSchemaRegistryContainer apicurioSchemaRegistryContainer;

    @BeforeAll
    static void startContainers() throws Exception {
        testNetwork = Network.newNetwork();
        confluentKafkaContainer = new org.testcontainers.containers.KafkaContainer(AgentTestUtil.CONFLUENT_KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka-confluent");
        confluentKafkaContainer.start();
        ossKafkaContainer = new KafkaContainer(AgentTestUtil.OSS_KAFKA_IMAGE)
                .withNetwork(testNetwork)
                .withNetworkAliases("kafka-oss");
        ossKafkaContainer.start();

        File kafkaConf = File.createTempFile("cdc-kafka-agent-", ".conf");
        kafkaConf.deleteOnExit();
        try (FileWriter fw = new FileWriter(kafkaConf)) {
            fw.write("bootstrapServers=kafka:9092\n");
        }

        String agentParams = String.format(
                "platform=KAFKA,kafkaConfigFile=%s,topicPrefix=events-", CONTAINER_KAFKA_CONFIG_PATH);
        DockerImageName cassandraImage = DockerImageName.parse(
                Optional.ofNullable(System.getenv("CASSANDRA_IMAGE")).orElse("cassandra:4.0.4"))
                .asCompatibleSubstituteFor("cassandra");
        cassandraContainer = CassandraContainer.createCassandraContainerWithAgent(
                cassandraImage, testNetwork, 1,
                System.getProperty("agentBuildDir"), "agent-c4", agentParams, "c4");
        cassandraContainer.withCopyFileToContainer(
                MountableFile.forHostPath(kafkaConf.getAbsolutePath()), CONTAINER_KAFKA_CONFIG_PATH);
        cassandraContainer.start();

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = "
                    + "{'class':'SimpleStrategy','replication_factor':1}");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl1_confluent (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl1_oss (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_evolve_confluent (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_evolve_oss (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            // One table per (registry × kafka-platform) pair so that no two parameterized
            // invocations share the same events-ks1.<table> topic.
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_confluent_confluent (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_confluent_oss (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_apicurio_confluent (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_apicurio_oss (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_evolve_confluent_confluent (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_evolve_confluent_oss (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_evolve_apicurio_confluent (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
            session.execute("CREATE TABLE IF NOT EXISTS ks1.tbl_schema_registry_evolve_apicurio_oss (a text, b text, PRIMARY KEY (a)) WITH cdc=true");
        }

        // Two registries, run against the same tests via schemaRegistries() below. Apicurio is
        // Apache 2.0 (server included); Confluent's server code (the core module of
        // confluentinc/schema-registry) is Confluent Community License, not Apache 2.0 -- see
        // ConfluentSchemaRegistryContainer's javadoc. Both are proven here because the connector's
        // serializer (io.confluent.kafka.serializers.KafkaAvroSerializer, itself Apache 2.0) talks
        // to either one with zero code differences -- Apicurio implements Confluent's own wire
        // protocol at /apis/ccompat/v7, verified directly against a real request/response.
        confluentSchemaRegistryContainer = new ConfluentSchemaRegistryContainer(
                DockerImageName.parse("confluentinc/cp-schema-registry:7.4.0"))
                .withNetworkAlias("schema-registry-confluent", testNetwork)
                .withKafkaBootstrapServers("kafka:9092");
        confluentSchemaRegistryContainer.start();

        apicurioSchemaRegistryContainer = new ApicurioSchemaRegistryContainer(
                DockerImageName.parse("apicurio/apicurio-registry:3.3.3"))
                .withNetworkAlias("schema-registry-apicurio", testNetwork);
        apicurioSchemaRegistryContainer.start();
    }

    // Deliberately resolves the URL here rather than passing the container itself as the
    // argument: since JUnit 5.10, @ParameterizedTest auto-closes any AutoCloseable/Startable
    // argument after each parameterized invocation (see
    // ParameterizedTestParameterResolver.CloseableArgument) -- fine for a container created
    // fresh per invocation, but these are shared static containers reused by two separate
    // @ParameterizedTest methods, so JUnit closing one after the first method's invocations
    // finish would leave the second method's invocations calling into a stopped container. A
    // plain String isn't AutoCloseable, so it sidesteps the problem entirely.
    private static Stream<Arguments> schemaRegistries() {
        return Stream.of(
                Arguments.of(confluentSchemaRegistryContainer.getSchemaRegistryUrl(), "confluent"),
                Arguments.of(apicurioSchemaRegistryContainer.getSchemaRegistryUrl(), "apicurio"));
    }

    // Same pattern as schemaRegistries(): passes bootstrap servers as a plain String rather than
    // the container itself to avoid JUnit 5.10+ auto-closing shared static containers between
    // parameterized invocations.
    private static Stream<Arguments> kafkaContainers() {
        return Stream.of(
                Arguments.of(confluentKafkaContainer.getBootstrapServers(), "confluent"),
                Arguments.of(ossKafkaContainer.getBootstrapServers(), "oss"));
    }

    // Cartesian product of both registries × both Kafka platforms: 4 invocations per test.
    // Arguments: (registryUrl, registryName, bootstrapServers, kafkaPlatform)
    private static Stream<Arguments> schemaRegistriesAndKafkaContainers() {
        return Stream.of(
                Arguments.of(confluentSchemaRegistryContainer.getSchemaRegistryUrl(), "confluent", confluentKafkaContainer.getBootstrapServers(), "confluent"),
                Arguments.of(confluentSchemaRegistryContainer.getSchemaRegistryUrl(), "confluent", ossKafkaContainer.getBootstrapServers(),       "oss"),
                Arguments.of(apicurioSchemaRegistryContainer.getSchemaRegistryUrl(),  "apicurio",  confluentKafkaContainer.getBootstrapServers(), "confluent"),
                Arguments.of(apicurioSchemaRegistryContainer.getSchemaRegistryUrl(),  "apicurio",  ossKafkaContainer.getBootstrapServers(),       "oss"));
    }

    @AfterAll
    static void stopContainers() {
        if (confluentSchemaRegistryContainer != null) {
            confluentSchemaRegistryContainer.close();
        }
        if (apicurioSchemaRegistryContainer != null) {
            apicurioSchemaRegistryContainer.close();
        }
        if (cassandraContainer != null) {
            cassandraContainer.close();
        }
        if (confluentKafkaContainer != null) {
            confluentKafkaContainer.close();
        }
        if (ossKafkaContainer != null) {
            ossKafkaContainer.close();
        }
        if (testNetwork != null) {
            testNetwork.close();
        }
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("kafkaContainers")
    void should_read_back_row_inserted_after_cdc_event(String bootstrapServers, String kafkaPlatform) throws Exception {
        String table = "tbl1_" + kafkaPlatform;
        String eventsTopic = "events-ks1." + table;
        String outputTopic = "data-ks1." + table;

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("INSERT INTO ks1." + table + " (a, b) VALUES ('hello', 'world')");
        }

        KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer(bootstrapServers, eventsTopic);

        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, table)
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, outputTopic)
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .build());
        task.mutationCache = new com.datastax.oss.cdc.MutationCache<>(3, 1000, Duration.ofMinutes(5));
        task.eventsTopic = eventsTopic;
        task.outputTopic = outputTopic;
        task.consumer = consumer;
        task.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor-it")
                .numThreads(1)
                .build();
        task.initCassandraClientWithRetry();

        try {
            List<SourceRecord> records = pollUntilNonEmpty(task, 30);

            assertThat(records).hasSize(1);
            SourceRecord record = records.get(0);
            assertThat(record.topic()).isEqualTo(outputTopic);

            GenericRecord row = decodeAvro(table, (byte[]) record.value());
            assertThat(row.get("b").toString()).isEqualTo("world");
        } finally {
            task.stop();
        }
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("kafkaContainers")
    void should_pick_up_altered_column_and_still_decode_downstream_correctly(String bootstrapServers, String kafkaPlatform) throws Exception {
        String table = "tbl_schema_evolve_" + kafkaPlatform;
        String eventsTopic = "events-ks1." + table;
        String outputTopic = "data-ks1." + table;

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("INSERT INTO ks1." + table + " (a, b) VALUES ('row1', 'before')");
        }

        KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer(bootstrapServers, eventsTopic);
        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, table)
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, outputTopic)
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .build());
        task.mutationCache = new com.datastax.oss.cdc.MutationCache<>(3, 1000, Duration.ofMinutes(5));
        task.eventsTopic = eventsTopic;
        task.outputTopic = outputTopic;
        task.consumer = consumer;
        task.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor-it")
                .numThreads(1)
                .build();
        task.initCassandraClientWithRetry();

        try {
            List<SourceRecord> beforeAlter = pollUntilNonEmpty(task, 30);
            assertThat(beforeAlter).hasSize(1);
            GenericRecord rowBeforeAlter = decodeAvro(table, (byte[]) beforeAlter.get(0).value());
            assertThat(rowBeforeAlter.get("b").toString()).isEqualTo("before");

            try (CqlSession session = cassandraContainer.getCqlSession()) {
                session.execute("ALTER TABLE ks1." + table + " ADD c text");
            }
            // The CQL driver's SchemaChangeListener fires asynchronously (schema agreement across
            // the cluster) and swaps task.valueConverterAndQuery in place - wait for that swap
            // rather than assuming it's immediate.
            waitUntilValueConverterCovers(task, "c", 30);

            try (CqlSession session = cassandraContainer.getCqlSession()) {
                session.execute("INSERT INTO ks1." + table + " (a, b, c) VALUES ('row2', 'before2', 'after')");
            }
            List<SourceRecord> afterAlter = pollUntilNonEmpty(task, 30);
            assertThat(afterAlter).hasSize(1);

            // Simulates a downstream consumer: there is no schema ID embedded in the record (see
            // the schema-evolution TODO on buildSourceRecord), so a real consumer has to know out
            // of band to re-fetch the current table schema in order to decode a post-alter record.
            GenericRecord rowAfterAlter = decodeAvro(table, (byte[]) afterAlter.get(0).value());
            assertThat(rowAfterAlter.get("b").toString()).isEqualTo("before2");
            assertThat(rowAfterAlter.get("c").toString()).isEqualTo("after");
        } finally {
            task.stop();
        }
    }

    @ParameterizedTest(name = "{1}-registry/{3}-kafka")
    @MethodSource("schemaRegistriesAndKafkaContainers")
    void should_register_schema_in_real_registry_and_decode_via_its_rest_api(
            String registryUrl, String registryName, String bootstrapServers, String kafkaPlatform) throws Exception {
        String table = "tbl_schema_registry_" + registryName + "_" + kafkaPlatform;
        String eventsTopic = "events-ks1." + table;
        String outputTopic = "data-ks1." + table;

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("INSERT INTO ks1." + table + " (a, b) VALUES ('hello', 'world')");
        }

        KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer(bootstrapServers, eventsTopic);
        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, table)
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, outputTopic)
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl)
                .build());
        task.mutationCache = new com.datastax.oss.cdc.MutationCache<>(3, 1000, Duration.ofMinutes(5));
        task.eventsTopic = eventsTopic;
        task.outputTopic = outputTopic;
        task.consumer = consumer;
        task.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor-it")
                .numThreads(1)
                .build();
        // Mirrors exactly what KafkaCassandraSourceTask#start does when schema.registry.url is
        // set (see the Preconditions.checkArgument right above it): this test bypasses start()
        // itself (it needs a real Kafka Connect SourceTaskContext, unavailable outside an
        // EmbeddedConnectCluster), but reuses the same SchemaRegistryProperties.build() the real
        // code path uses, so this is exercising real production wiring against a real registry,
        // not a hand-rolled substitute.
        task.schemaRegistrySerializer = new KafkaAvroSerializer();
        task.schemaRegistrySerializer.configure(SchemaRegistryProperties.build(task.config), false);
        task.initCassandraClientWithRetry();

        try {
            List<SourceRecord> records = pollUntilNonEmpty(task, 30);
            assertThat(records).hasSize(1);
            byte[] valueBytes = (byte[]) records.get(0).value();

            // Confluent wire format: magic byte 0x0, then a 4-byte big-endian schema id.
            assertThat(valueBytes[0]).isEqualTo((byte) 0);
            int schemaId = ByteBuffer.wrap(valueBytes, 1, 4).getInt();

            HttpClient http = HttpClient.newHttpClient();
            ObjectMapper mapper = new ObjectMapper();

            // Proves the schema was actually registered over the wire -- not just that the
            // serializer thinks it registered something -- by asking the registry's own REST
            // API, independently of anything the connector/task remembers in memory.
            HttpResponse<String> versions = http.send(
                    HttpRequest.newBuilder(URI.create(registryUrl + "/subjects/" + outputTopic + "-value/versions")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(versions.statusCode()).isEqualTo(200);
            assertThat(mapper.readTree(versions.body()).toString()).contains("1");

            // Decodes using the schema fetched back from the registry by ID -- rather than a
            // schema reconstructed locally from table metadata -- to prove a real, independent
            // consumer could decode this record knowing only the registry URL and the bytes.
            HttpResponse<String> schemaById = http.send(
                    HttpRequest.newBuilder(URI.create(registryUrl + "/schemas/ids/" + schemaId)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(schemaById.statusCode()).isEqualTo(200);
            JsonNode schemaJson = mapper.readTree(schemaById.body());
            Schema avroSchema = new Schema.Parser().parse(schemaJson.get("schema").asText());

            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(valueBytes, 5, valueBytes.length - 5, null);
            GenericRecord row = new GenericDatumReader<GenericRecord>(avroSchema).read(null, decoder);
            assertThat(row.get("b").toString()).isEqualTo("world");
        } finally {
            task.stop();
        }
    }

    // Combines the two things should_pick_up_altered_column_and_still_decode_downstream_correctly
    // and should_register_schema_in_real_registry_and_decode_via_its_rest_api each cover
    // separately: this is the actual reason schema.registry.url exists (see the comment on
    // setValueConverterAndQuery) -- without it, an ALTER TABLE just swaps the schema in place
    // with no check; with it, the new schema is supposed to really land in the registry as a new
    // version. Proves both halves of that claim against a real registry, not by inspecting the
    // task's in-memory state.
    @ParameterizedTest(name = "{1}")
    @MethodSource("schemaRegistries")
    void should_register_new_schema_version_after_table_alter(
            String registryUrl, String registryName) throws Exception {
        String table = "tbl_schema_registry_evolve_" + registryName;
        String eventsTopic = "events-ks1." + table;
        String outputTopic = "data-ks1." + table;

        try (CqlSession session = cassandraContainer.getCqlSession()) {
            session.execute("INSERT INTO ks1." + table + " (a, b) VALUES ('row1', 'before')");
        }

        KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer(confluentKafkaContainer.getBootstrapServers(), eventsTopic);
        KafkaCassandraSourceTask task = new KafkaCassandraSourceTask();
        task.config = new CassandraSourceConnectorConfig(ImmutableMap.<String, String>builder()
                .put(CassandraSourceConnectorConfig.KEYSPACE_NAME_CONFIG, "ks1")
                .put(CassandraSourceConnectorConfig.TABLE_NAME_CONFIG, table)
                .put(CassandraSourceConnectorConfig.EVENTS_TOPIC_NAME_CONFIG, eventsTopic)
                .put(CassandraSourceConnectorConfig.OUTPUT_TOPIC_CONFIG, outputTopic)
                .put(CassandraSourceConnectorConfig.CONTACT_POINTS_OPT, cassandraContainer.getHost())
                .put(CassandraSourceConnectorConfig.PORT_OPT,
                        String.valueOf(cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT)))
                .put(CassandraSourceConnectorConfig.DC_OPT, cassandraContainer.getLocalDc())
                .put(CassandraSourceConnectorConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl)
                .build());
        task.mutationCache = new com.datastax.oss.cdc.MutationCache<>(3, 1000, Duration.ofMinutes(5));
        task.eventsTopic = eventsTopic;
        task.outputTopic = outputTopic;
        task.consumer = consumer;
        task.queryExecutor = OrderedExecutor.newBuilder()
                .name("cdc-query-executor-it")
                .numThreads(1)
                .build();
        task.schemaRegistrySerializer = new KafkaAvroSerializer();
        task.schemaRegistrySerializer.configure(SchemaRegistryProperties.build(task.config), false);
        task.initCassandraClientWithRetry();

        try {
            List<SourceRecord> beforeAlter = pollUntilNonEmpty(task, 30);
            assertThat(beforeAlter).hasSize(1);
            byte[] beforeBytes = (byte[]) beforeAlter.get(0).value();
            int schemaIdV1 = ByteBuffer.wrap(beforeBytes, 1, 4).getInt();

            List<Integer> versionsBeforeAlter = fetchVersions(registryUrl, outputTopic);
            assertThat(versionsBeforeAlter).containsExactly(1);
            Schema schemaV1 = fetchSchemaById(registryUrl, schemaIdV1);

            try (CqlSession session = cassandraContainer.getCqlSession()) {
                session.execute("ALTER TABLE ks1." + table + " ADD c text");
            }
            waitUntilValueConverterCovers(task, "c", 30);

            try (CqlSession session = cassandraContainer.getCqlSession()) {
                session.execute("INSERT INTO ks1." + table + " (a, b, c) VALUES ('row2', 'before2', 'after')");
            }
            List<SourceRecord> afterAlter = pollUntilNonEmpty(task, 30);
            assertThat(afterAlter).hasSize(1);
            byte[] afterBytes = (byte[]) afterAlter.get(0).value();
            int schemaIdV2 = ByteBuffer.wrap(afterBytes, 1, 4).getInt();

            // The actual proof the registry was consulted, not just the task's own in-memory
            // converter: a genuinely new version registered under the same subject, independent
            // of anything the task remembers.
            assertThat(schemaIdV2).isNotEqualTo(schemaIdV1);
            List<Integer> versionsAfterAlter = fetchVersions(registryUrl, outputTopic);
            assertThat(versionsAfterAlter).containsExactly(1, 2);

            Schema schemaV2 = fetchSchemaById(registryUrl, schemaIdV2);
            assertThat(schemaV2.getFields()).hasSize(schemaV1.getFields().size() + 1);

            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(afterBytes, 5, afterBytes.length - 5, null);
            GenericRecord rowAfterAlter = new GenericDatumReader<GenericRecord>(schemaV2).read(null, decoder);
            assertThat(rowAfterAlter.get("b").toString()).isEqualTo("before2");
            assertThat(rowAfterAlter.get("c").toString()).isEqualTo("after");
        } finally {
            task.stop();
        }
    }

    private List<Integer> fetchVersions(String registryUrl, String outputTopic) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(registryUrl + "/subjects/" + outputTopic + "-value/versions")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode arr = new ObjectMapper().readTree(response.body());
        List<Integer> versions = new java.util.ArrayList<>();
        arr.forEach(node -> versions.add(node.asInt()));
        return versions;
    }

    private Schema fetchSchemaById(String registryUrl, int schemaId) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(registryUrl + "/schemas/ids/" + schemaId)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode schemaJson = new ObjectMapper().readTree(response.body());
        return new Schema.Parser().parse(schemaJson.get("schema").asText());
    }

    private void waitUntilValueConverterCovers(KafkaCassandraSourceTask task, String columnName, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            ConverterAndQuery<?> valueConverterAndQuery = task.valueConverterAndQuery;
            if (valueConverterAndQuery != null && valueConverterAndQuery.getConverter() instanceof AvroRowConverter) {
                org.apache.avro.Schema schema = ((AvroRowConverter) valueConverterAndQuery.getConverter()).nativeSchema;
                if (schema.getField(columnName) != null) {
                    return;
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for value converter to pick up column " + columnName);
    }

    private KafkaConsumer<byte[], byte[]> createInternalConsumer(String bootstrapServers, String eventsTopic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-cassandra-source-task-it");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        TopicPartition tp = new TopicPartition(eventsTopic, 0);
        consumer.assign(Collections.singletonList(tp));
        consumer.seekToBeginning(Collections.singletonList(tp));
        return consumer;
    }

    private List<SourceRecord> pollUntilNonEmpty(KafkaCassandraSourceTask task, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            List<SourceRecord> records = task.poll();
            if (!records.isEmpty()) {
                return records;
            }
        }
        return Collections.emptyList();
    }

    private GenericRecord decodeAvro(String tableName, byte[] bytes) throws Exception {
        try (CqlSession session = cassandraContainer.getCqlSession()) {
            com.datastax.oss.driver.api.core.metadata.schema.TableMetadata table =
                    session.getMetadata().getKeyspace("ks1").get().getTable(tableName).get();
            // the value converter's schema covers only non-primary-key columns (setValueConverterAndQuery)
            List<com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata> nonPkColumns =
                    table.getColumns().values().stream()
                            .filter(c -> !table.getPrimaryKey().contains(c))
                            .collect(java.util.stream.Collectors.toList());
            org.apache.avro.Schema schema = new AvroRowConverter(
                    session.getMetadata().getKeyspace("ks1").get(), table, nonPkColumns).nativeSchema;
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            return reader.read(null, decoder);
        }
    }
}
