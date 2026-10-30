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

package com.datastax.oss.cdc.backfill;

import com.datastax.oss.cdc.agent.AbstractMutation;
import com.datastax.oss.cdc.agent.KafkaMutationSender;
import com.datastax.oss.cdc.agent.MutationSender;
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.cdc.backfill.exporter.ExportedTable;
import com.datastax.oss.cdc.backfill.factory.ConnectorFactory;
import com.datastax.oss.cdc.backfill.factory.KafkaMutationSenderFactory;
import com.datastax.oss.cdc.backfill.factory.PulsarMutationSenderFactory;
import com.datastax.oss.cdc.backfill.importer.AbstractImporter;
import com.datastax.oss.cdc.backfill.importer.KafkaImporter;
import com.datastax.oss.cdc.backfill.importer.PulsarImporter;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.oss.cdc.backfill.importer.AbstractImporter.MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING;
import static org.assertj.core.api.Fail.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parameterized test that runs all importer scenarios against both
 * {@link PulsarImporter} and {@link KafkaImporter} for full parity.
 *
 * <p>Each {@code @ParameterizedTest} receives an {@link ImporterFixture} — a small
 * struct that hides the platform-specific mock types behind a uniform API. The
 * {@link #importerFixtures()} factory creates fresh mocks for every test invocation
 * so there is no shared state between runs.
 */
public class ImporterTest {

    // ── fixture ──────────────────────────────────────────────────────────────

    /**
     * Per-test fixture: hides which platform is under test behind a uniform API.
     * {@link #verifySenderClose()} routes the close verification to the correct
     * concrete mock type, avoiding a cast to {@code AutoCloseable} (which would
     * require catching checked {@code Exception}).
     */
    static abstract class ImporterFixture {
        final String name;
        /** The mock sender returned by the factory; used for interaction verification. */
        final MutationSender<TableMetadata> sender;

        ImporterFixture(String name, MutationSender<TableMetadata> sender) {
            this.name = name;
            this.sender = sender;
        }

        /** Creates a fresh mock of the same concrete sender type for controlled-future tests. */
        abstract MutationSender<TableMetadata> newControlledSender();

        /** Build the importer under test with the given connector and table. */
        abstract AbstractImporter build(ConnectorFactory cf, ExportedTable et);

        /**
         * Build a replacement importer that uses a fresh controlled sender (not the
         * pre-built fixture one) — used for back-pressure / fail-fast tests.
         */
        abstract AbstractImporter buildWith(ConnectorFactory cf, ExportedTable et,
                                            MutationSender<TableMetadata> controlledSender);

        /** Verify close() was called once on the given sender without an unchecked cast. */
        abstract void verifySenderClose(MutationSender<TableMetadata> s);

        @Override public String toString() { return name; }
    }

    static class PulsarFixture extends ImporterFixture {
        private final PulsarMutationSenderFactory factory;

        PulsarFixture(PulsarMutationSender sender, PulsarMutationSenderFactory factory) {
            super("Pulsar", sender);
            this.factory = factory;
        }

        @Override
        public MutationSender<TableMetadata> newControlledSender() {
            return Mockito.mock(PulsarMutationSender.class);
        }

        @Override
        public AbstractImporter build(ConnectorFactory cf, ExportedTable et) {
            return new PulsarImporter(cf, et, factory);
        }

        @Override
        public AbstractImporter buildWith(ConnectorFactory cf, ExportedTable et,
                                          MutationSender<TableMetadata> controlledSender) {
            PulsarMutationSenderFactory f = Mockito.mock(PulsarMutationSenderFactory.class);
            Mockito.when(f.newPulsarMutationSender()).thenReturn((PulsarMutationSender) controlledSender);
            return new PulsarImporter(cf, et, f);
        }

        @Override
        public void verifySenderClose(MutationSender<TableMetadata> s) {
            Mockito.verify((PulsarMutationSender) s, Mockito.times(1)).close();
        }
    }

    static class KafkaFixture extends ImporterFixture {
        private final KafkaMutationSenderFactory factory;

        KafkaFixture(KafkaMutationSender sender, KafkaMutationSenderFactory factory) {
            super("Kafka", sender);
            this.factory = factory;
        }

        @Override
        public MutationSender<TableMetadata> newControlledSender() {
            return Mockito.mock(KafkaMutationSender.class);
        }

        @Override
        public AbstractImporter build(ConnectorFactory cf, ExportedTable et) {
            return new KafkaImporter(cf, et, factory);
        }

        @Override
        public AbstractImporter buildWith(ConnectorFactory cf, ExportedTable et,
                                          MutationSender<TableMetadata> controlledSender) {
            KafkaMutationSenderFactory f = Mockito.mock(KafkaMutationSenderFactory.class);
            Mockito.when(f.newKafkaMutationSender()).thenReturn((KafkaMutationSender) controlledSender);
            return new KafkaImporter(cf, et, f);
        }

        @Override
        public void verifySenderClose(MutationSender<TableMetadata> s) {
            Mockito.verify((KafkaMutationSender) s, Mockito.times(1)).close();
        }
    }

    /** Provides one fresh fixture per platform for each test invocation. */
    static Stream<Arguments> importerFixtures() {
        // Pulsar
        PulsarMutationSender pulsarSender = Mockito.mock(PulsarMutationSender.class);
        Mockito.when(pulsarSender.sendMutationAsync(Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        PulsarMutationSenderFactory pulsarFactory = Mockito.mock(PulsarMutationSenderFactory.class);
        Mockito.when(pulsarFactory.newPulsarMutationSender()).thenReturn(pulsarSender);

        // Kafka
        KafkaMutationSender kafkaSender = Mockito.mock(KafkaMutationSender.class);
        Mockito.when(kafkaSender.sendMutationAsync(Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        KafkaMutationSenderFactory kafkaFactory = Mockito.mock(KafkaMutationSenderFactory.class);
        Mockito.when(kafkaFactory.newKafkaMutationSender()).thenReturn(kafkaSender);

        return Stream.of(
                Arguments.of(new PulsarFixture(pulsarSender, pulsarFactory)),
                Arguments.of(new KafkaFixture(kafkaSender, kafkaFactory))
        );
    }

    // ── tests ────────────────────────────────────────────────────────────────

    @ParameterizedTest(name = "{0}")
    @MethodSource("importerFixtures")
    public void testImportPartitionKeyOnly(ImporterFixture fixture) {
        // given
        ExportedTable exportedTable = Mockito.mock(ExportedTable.class);
        TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
        ConnectorFactory connectorFactory = new ConnectorFactory(Paths.get(url("sample-001.csv")));
        AbstractImporter importer = fixture.build(connectorFactory, exportedTable);

        List<ColumnMetadata> cassandraColumns = new ArrayList<>();
        cassandraColumns.add(new ColumnMetadata("ks1", "key",
                new ColumnIdentifier("key", true), UTF8Type.instance, 0, ColumnMetadata.Kind.PARTITION_KEY));
        Mockito.when(tableMetadata.primaryKeyColumns()).thenReturn(cassandraColumns);
        Mockito.when(exportedTable.getCassandraTable()).thenReturn(tableMetadata);

        List<com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata> columns = new ArrayList<>();
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"),
                CqlIdentifier.fromInternal("key"), DataTypes.ASCII, false));
        Mockito.when(exportedTable.getPrimaryKey()).thenReturn(columns);

        // when
        ExitStatus status = importer.importTable();

        // then
        assertEquals(ExitStatus.STATUS_OK, status);
        ArgumentCaptor<AbstractMutation<TableMetadata>> captor = captorFor();
        Mockito.verify(fixture.sender, Mockito.times(2)).sendMutationAsync(captor.capture());
        fixture.verifySenderClose(fixture.sender);
        List<AbstractMutation<TableMetadata>> mutations = captor.getAllValues();
        assertEquals(2, mutations.size());
        assertEquals(-1L, mutations.get(0).getTs());
        assertEquals(-1L, mutations.get(1).getTs());
        List<Object> allPkValues = mutations.stream()
                .flatMap(v -> Arrays.stream(v.getPkValues())).collect(Collectors.toList());
        assertThat(allPkValues, containsInAnyOrder("id3", "id8"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("importerFixtures")
    public void testImportPartitionAndClusteringKeys(ImporterFixture fixture) {
        // given
        ExportedTable exportedTable = Mockito.mock(ExportedTable.class);
        TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
        ConnectorFactory connectorFactory = new ConnectorFactory(Paths.get(url("sample-002.csv")));
        AbstractImporter importer = fixture.build(connectorFactory, exportedTable);

        List<ColumnMetadata> cassandraColumns = new ArrayList<>();
        cassandraColumns.add(new ColumnMetadata("ks1", "xtext",      new ColumnIdentifier("xtext",      true), UTF8Type.instance,       0, ColumnMetadata.Kind.PARTITION_KEY));
        cassandraColumns.add(new ColumnMetadata("ks1", "xboolean",   new ColumnIdentifier("xboolean",   true), BooleanType.instance,    1, ColumnMetadata.Kind.CLUSTERING));
        cassandraColumns.add(new ColumnMetadata("ks1", "xint",       new ColumnIdentifier("xint",       true), IntegerType.instance,    2, ColumnMetadata.Kind.CLUSTERING));
        cassandraColumns.add(new ColumnMetadata("ks1", "xtime",      new ColumnIdentifier("xtime",      true), TimeType.instance,       3, ColumnMetadata.Kind.CLUSTERING));
        cassandraColumns.add(new ColumnMetadata("ks1", "xdate",      new ColumnIdentifier("xdate",      true), SimpleDateType.instance, 4, ColumnMetadata.Kind.CLUSTERING));
        cassandraColumns.add(new ColumnMetadata("ks1", "xblob",      new ColumnIdentifier("xblob",      true), BytesType.instance,      5, ColumnMetadata.Kind.CLUSTERING));
        cassandraColumns.add(new ColumnMetadata("ks1", "xtimestamp", new ColumnIdentifier("xtimestamp", true), TimestampType.instance,  6, ColumnMetadata.Kind.CLUSTERING));
        cassandraColumns.add(new ColumnMetadata("ks1", "xuuid",      new ColumnIdentifier("xuuid",      true), UUIDType.instance,       7, ColumnMetadata.Kind.CLUSTERING));
        Mockito.when(tableMetadata.primaryKeyColumns()).thenReturn(cassandraColumns);
        Mockito.when(exportedTable.getCassandraTable()).thenReturn(tableMetadata);

        List<com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata> columns = new ArrayList<>();
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xtext"),      DataTypes.TEXT,      false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xboolean"),   DataTypes.BOOLEAN,   false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xint"),       DataTypes.INT,       false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xtime"),      DataTypes.TIME,      false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xdate"),      DataTypes.DATE,      false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xblob"),      DataTypes.BLOB,      false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xtimestamp"), DataTypes.TIMESTAMP, false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xuuid"),      DataTypes.UUID,      false));
        Mockito.when(exportedTable.getPrimaryKey()).thenReturn(columns);

        // when
        ExitStatus status = importer.importTable();

        // then
        assertEquals(ExitStatus.STATUS_OK, status);
        ArgumentCaptor<AbstractMutation<TableMetadata>> captor = captorFor();
        Mockito.verify(fixture.sender, Mockito.times(2)).sendMutationAsync(captor.capture());
        fixture.verifySenderClose(fixture.sender);
        List<AbstractMutation<TableMetadata>> mutations = captor.getAllValues();
        assertEquals(2, mutations.size());
        List<Object>[] allPkValues = mutations.stream().map(v -> v.getPkValues()).map(Arrays::asList).toArray(List[]::new);
        assertThat(allPkValues[0], containsInRelativeOrder("vtext", true, 2, LocalTime.of(1, 2, 3).toNanoOfDay(),
                ByteBuffer.wrap(new byte[]{0x00, 0x01}), Instant.parse("2023-03-22T18:16:20.808Z"),
                UUID.fromString("3920dd7d-dcbf-4c2e-bbe5-f300b720ae0d")));
        assertEquals(LocalDate.of(2023, 3, 2), cqlSimpleDateToLocalDate((Integer) allPkValues[0].get(4)));
        assertThat(allPkValues[1], containsInRelativeOrder("v2text", false, 3, LocalTime.of(1, 2, 4).toNanoOfDay(),
                ByteBuffer.wrap(new byte[]{0x01}), Instant.parse("2022-02-21T18:16:20.807Z"),
                UUID.fromString("19296adf-fa87-4ba2-bad8-ae86d2769ee6")));
        assertEquals(LocalDate.of(2023, 3, 1), cqlSimpleDateToLocalDate((Integer) allPkValues[1].get(4)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("importerFixtures")
    public void testImportInflightMessagesBound(ImporterFixture fixture) throws Exception {
        // given
        Connector connector = Mockito.mock(Connector.class);
        Resource resource = Mockito.mock(Resource.class);
        Record record = Mockito.mock(Record.class);
        Record[] records = new Record[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING / 2 + 1];
        Arrays.fill(records, record);
        Mockito.when(resource.read()).thenReturn(Flux.just(records));
        Mockito.when(connector.read()).thenReturn(Flux.just(resource, resource));

        ConnectorFactory connectorFactory = Mockito.mock(ConnectorFactory.class);
        Mockito.when(connectorFactory.newCVSConnector()).thenReturn(connector);

        // use a controlled sender with individually completable futures
        MutationSender<TableMetadata> controlledSender = fixture.newControlledSender();
        CompletableFuture<Void>[] futures = buildFutures(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 2);
        AtomicInteger idx = new AtomicInteger();
        Mockito.doAnswer(inv -> futures[idx.getAndIncrement()]).when(controlledSender).sendMutationAsync(Mockito.any());

        ExportedTable exportedTable = Mockito.mock(ExportedTable.class);
        AbstractImporter importer = fixture.buildWith(connectorFactory, exportedTable, controlledSender);

        // when
        CompletableFuture<ExitStatus> importFuture = CompletableFuture.supplyAsync(() -> importer.importTable());

        // blocked while MAX in-flight
        assertImportBlocked(importFuture);
        Mockito.verify(controlledSender, Mockito.times(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING))
                .sendMutationAsync(Mockito.any());

        // release MAX → unblocks remaining 2
        for (int i = 0; i < MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING; i++) futures[i].complete(null);
        assertImportBlocked(importFuture);
        Mockito.verify(controlledSender, Mockito.times(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 2))
                .sendMutationAsync(Mockito.any());

        // release all-but-last → still blocked (1 in-flight)
        futures[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING].complete(null);
        assertImportBlocked(importFuture);

        // release last → unblocked
        futures[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 1].complete(null);
        assertImportUnBlocked(importFuture);

        assertTrue(importFuture.isDone());
        assertThat(importFuture.get(), is(ExitStatus.STATUS_OK));
        fixture.verifySenderClose(controlledSender);
        Mockito.verifyNoMoreInteractions(controlledSender);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("importerFixtures")
    public void testImportFailsFast(ImporterFixture fixture) throws Exception {
        // given
        Connector connector = Mockito.mock(Connector.class);
        Resource resource = Mockito.mock(Resource.class);
        Record record = Mockito.mock(Record.class);
        Record[] records = new Record[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING * 2];
        Arrays.fill(records, record);
        Mockito.when(resource.read()).thenReturn(Flux.just(records));
        Mockito.when(connector.read()).thenReturn(Flux.just(resource));

        ConnectorFactory connectorFactory = Mockito.mock(ConnectorFactory.class);
        Mockito.when(connectorFactory.newCVSConnector()).thenReturn(connector);

        MutationSender<TableMetadata> controlledSender = fixture.newControlledSender();
        CompletableFuture<Void>[] futures = buildFutures(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING * 2);
        AtomicInteger idx = new AtomicInteger();
        Mockito.doAnswer(inv -> futures[idx.getAndIncrement()]).when(controlledSender).sendMutationAsync(Mockito.any());

        ExportedTable exportedTable = Mockito.mock(ExportedTable.class);
        AbstractImporter importer = fixture.buildWith(connectorFactory, exportedTable, controlledSender);

        // when
        CompletableFuture<ExitStatus> importFuture = CompletableFuture.supplyAsync(() -> importer.importTable());
        assertImportBlocked(importFuture);

        // 5th future fails → fail-fast
        futures[4].completeExceptionally(new RuntimeException("poison pill"));
        assertImportUnBlocked(importFuture);

        // then
        assertTrue(importFuture.isDone());
        fixture.verifySenderClose(controlledSender);
        assertThat(importFuture.get(), is(ExitStatus.STATUS_ABORTED_FATAL_ERROR));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private CompletableFuture<Void>[] buildFutures(int count) {
        CompletableFuture<Void>[] futures = new CompletableFuture[count];
        for (int i = 0; i < count; i++) futures[i] = new CompletableFuture<>();
        return futures;
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<AbstractMutation<TableMetadata>> captorFor() {
        return ArgumentCaptor.forClass((Class) AbstractMutation.class);
    }

    private void assertImportUnBlocked(CompletableFuture<ExitStatus> f) {
        try {
            f.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail("Import should have completed, got: " + e);
            throw new RuntimeException(e);
        }
    }

    private void assertImportBlocked(CompletableFuture<ExitStatus> f) {
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Import should have timed out but completed");
        } catch (InterruptedException e) {
            fail("Import should have timed out, got interrupted");
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            fail("Import should have timed out, got execution exception");
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            // expected
        }
    }

    private static String url(String resource) {
        return StringUtils.quoteJson(rawURL("/" + resource));
    }

    private static URL rawURL(String resource) {
        return ImporterTest.class.getResource(resource);
    }

    private static LocalDate cqlSimpleDateToLocalDate(int value) {
        long timeInMillis = Duration.ofDays(value + Integer.MIN_VALUE).toMillis();
        Instant instant = Instant.ofEpochMilli(timeInMillis);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
    }
}
