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
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.cdc.backfill.exporter.ExportedTable;
import com.datastax.oss.cdc.backfill.factory.ConnectorFactory;
import com.datastax.oss.cdc.backfill.factory.PulsarMutationSenderFactory;
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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.shade.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.net.URISyntaxException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.datastax.oss.cdc.backfill.importer.PulsarImporter.MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING;
import static org.assertj.core.api.Fail.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PulsarImporterTest {
    @Mock
    private TableMetadata tableMetadata;

    @Mock
    private ExportedTable exportedTable;

    @Mock
    private PulsarMutationSenderFactory factory;

    @Mock
    private PulsarMutationSender sender;

    @Captor
    private ArgumentCaptor<AbstractMutation<TableMetadata>> abstractMutationCaptor;
    private PulsarImporter importer;

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);

        Mockito.when(sender.sendMutationAsync(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(factory.newPulsarMutationSender()).thenReturn(sender);
    }

    @Test
    public void testImportPartitionKeyOnly() {
        // given
        String fileName = "sample-001.csv";
        ConnectorFactory connectorFactory = new ConnectorFactory(Paths.get(url(fileName)));
        importer = new PulsarImporter(connectorFactory, exportedTable, factory);

        List<ColumnMetadata> cassandraColumns = new ArrayList<>();
        ColumnIdentifier identifier = new ColumnIdentifier("key", true);
        ColumnMetadata columnMetadata = new ColumnMetadata("ks1", "key", identifier, UTF8Type.instance, 0, ColumnMetadata.Kind.PARTITION_KEY);
        cassandraColumns.add(columnMetadata);
        Mockito.when(tableMetadata.primaryKeyColumns()).thenReturn(cassandraColumns);

        Mockito.when(exportedTable.getCassandraTable()).thenReturn(tableMetadata);
        List<com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata > columns = new ArrayList<>();
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"),CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("key"), DataTypes.ASCII, false));Mockito.when(exportedTable.getPrimaryKey()).thenReturn(columns);

        // when
        ExitStatus status = importer.importTable();

        // then
        assertEquals(ExitStatus.STATUS_OK, status);
        Mockito.verify(sender, Mockito.times(2)).sendMutationAsync(abstractMutationCaptor.capture());
        List<AbstractMutation<TableMetadata>> pkValues = abstractMutationCaptor.getAllValues();
        assertEquals(2, pkValues.size());
        List<Object> allPkValues = pkValues.stream().flatMap(v-> Arrays.stream(v.getPkValues())).collect(Collectors.toList());
        assertThat(allPkValues, containsInAnyOrder("id3", "id8"));
    }

    @Test
    public void testImportPartitionAndClusteringKeys() {
        // given
        String fileName = "sample-002.csv";
        ConnectorFactory connectorFactory = new ConnectorFactory(Paths.get(url(fileName)));
        importer = new PulsarImporter(connectorFactory, exportedTable, factory);

        List<ColumnMetadata> cassandraColumns = new ArrayList<>();
        ColumnIdentifier xtextIdentifier =
                new ColumnIdentifier("xtext", true);
        ColumnMetadata xtextColumnMetadata =
                new ColumnMetadata("ks1", "xtext", xtextIdentifier, UTF8Type.instance, 0, ColumnMetadata.Kind.PARTITION_KEY);
        ColumnIdentifier xbooleanIdentifier =
                new ColumnIdentifier("xboolean", true);
        ColumnMetadata xbooleanColumnMetadata =
                new ColumnMetadata("ks1", "xboolean", xbooleanIdentifier, BooleanType.instance, 1, ColumnMetadata.Kind.CLUSTERING);
        ColumnIdentifier xintIdentifier =
                new ColumnIdentifier("xint", true);
        ColumnIdentifier xtimeIdentifier =
                new ColumnIdentifier("xtime", true);
        ColumnIdentifier xdateIdentifier =
                new ColumnIdentifier("xdate", true);
        ColumnIdentifier xblobIdentifier =
                new ColumnIdentifier("xblob", true);
        ColumnMetadata xintColumnMetadata =
                new ColumnMetadata("ks1", "xint", xintIdentifier, IntegerType.instance, 2, ColumnMetadata.Kind.CLUSTERING);
        ColumnMetadata xtimeColumnMetadata =
                new ColumnMetadata("ks1", "xtime", xtimeIdentifier, TimeType.instance, 3, ColumnMetadata.Kind.CLUSTERING);
        ColumnMetadata xdateColumnMetadata =
                new ColumnMetadata("ks1", "xdate", xdateIdentifier, SimpleDateType.instance, 4, ColumnMetadata.Kind.CLUSTERING);
        ColumnMetadata xblobColumnMetadata =
                new ColumnMetadata("ks1", "xblob", xblobIdentifier, BytesType.instance, 5, ColumnMetadata.Kind.CLUSTERING);
        cassandraColumns.add(xtextColumnMetadata);
        cassandraColumns.add(xbooleanColumnMetadata);
        cassandraColumns.add(xintColumnMetadata);
        cassandraColumns.add(xtimeColumnMetadata);
        cassandraColumns.add(xdateColumnMetadata);
        cassandraColumns.add(xblobColumnMetadata);

        Mockito.when(tableMetadata.primaryKeyColumns()).thenReturn(cassandraColumns);

        Mockito.when(exportedTable.getCassandraTable()).thenReturn(tableMetadata);
        List<com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata > columns = new ArrayList<>();
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xtext"), DataTypes.TEXT, false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xboolean"), DataTypes.BOOLEAN, false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xint"), DataTypes.INT, false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xtime"), DataTypes.TIME, false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xdate"), DataTypes.DATE, false));
        columns.add(new DefaultColumnMetadata(CqlIdentifier.fromInternal("ks1"), CqlIdentifier.fromInternal("table1"), CqlIdentifier.fromInternal("xblob"), DataTypes.BLOB, false));
        Mockito.when(exportedTable.getPrimaryKey()).thenReturn(columns);

        // when
        ExitStatus status = importer.importTable();

        // then
        assertEquals(ExitStatus.STATUS_OK, status);
        Mockito.verify(sender, Mockito.times(2)).sendMutationAsync(abstractMutationCaptor.capture());
        List<AbstractMutation<TableMetadata>> pkValues = abstractMutationCaptor.getAllValues();
        assertEquals(2, pkValues.size());
        List<Object>[] allPkValues = pkValues.stream().map(v-> v.getPkValues()).map(Arrays::asList).toArray(List[]::new);
        assertThat(allPkValues[0], containsInRelativeOrder("vtext", true, 2, LocalTime.of(1, 2, 3).toNanoOfDay(),
                ByteBuffer.wrap(new byte[]{0x00, 0x01})));
        assertEquals(LocalDate.of(2023, 3, 2), cqlSimpleDateToLocalDate((Integer) allPkValues[0].get(4)));
        assertThat(allPkValues[1], containsInRelativeOrder("v2text", false, 3, LocalTime.of(1, 2, 4).toNanoOfDay(),
                ByteBuffer.wrap(new byte[]{0x01})));
        assertEquals(LocalDate.of(2023, 3, 1), cqlSimpleDateToLocalDate((Integer) allPkValues[1].get(4)));
    }

    @Test
    public void testImportInflightMessagesBound() throws URISyntaxException, IOException {
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

        CompletableFuture<MessageId>[] futures = new CompletableFuture[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 2];
        for (int i = 0; i < MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 2; i++) {
            futures[i] = new CompletableFuture<>();
            // note that Arrays.fill(futures, new CompletableFuture<>()) will reuse the same future object
        }

        CompletableFuture<MessageId> beforeLastfuture = futures[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING];
        CompletableFuture<MessageId> lastFuture = futures[MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 1];

        //Mockito.reset(sender, factory);
        sender = Mockito.mock(PulsarMutationSender.class);
        factory = Mockito.mock(PulsarMutationSenderFactory.class);
        AtomicInteger futureIndex = new AtomicInteger();
        Mockito.doAnswer(invocation -> futures[futureIndex.getAndIncrement()]).when(sender).sendMutationAsync(Mockito.any());
        Arrays.stream(futures).forEach(f -> Mockito.when(sender.sendMutationAsync(Mockito.any())).thenReturn(f));
        Mockito.when(factory.newPulsarMutationSender()).thenReturn(sender);
        importer = new PulsarImporter(connectorFactory, exportedTable, factory);

        // when
        CompletableFuture<Void> importFuture = CompletableFuture.runAsync(() -> {
            importer.importTable();
        });

        // then
        // since MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 2 futures are in-flight, the import should be blocked
        assertImportBlocked(importFuture);

        // at this point, mutation sender should've been invoked exactly MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING times
        Mockito.verify(sender, Mockito.times(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING))
                .sendMutationAsync(Mockito.any());

        // release MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING - 1 futures
        for (int i = 0; i < MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING - 1; i++) {
            futures[i].complete(null);
        }

        assertImportBlocked(importFuture);

        // release another future. Although the memory is not full, there is still 1 future in-flight. The overall
        // import should still be blocked
        beforeLastfuture.complete(null);
        assertImportBlocked(importFuture);

        // release the last future. The import should be unblocked
        lastFuture.complete(null);
        assertImportUnBlocked(importFuture);

        Mockito.verify(sender, Mockito.times(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING + 2))
                .sendMutationAsync(Mockito.any());
    }

    private void assertImportUnBlocked(CompletableFuture<Void> importFuture) {
        try {
            importFuture.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            fail("Import should have been completed, got exception instead");
            throw new RuntimeException(e);
        }
    }

    private void assertImportBlocked(CompletableFuture<Void> importFuture) {
        try {
            importFuture.get(5, TimeUnit.SECONDS);
            fail("Import should have timed out");
        } catch (InterruptedException e) {
            fail("Import should have timed out, got interrupted instead");
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            fail("Import should have timed out, got execution exception instead");
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            // expected
        }
    }

    private static String url(String resource) {
        return StringUtils.quoteJson(rawURL("/" +resource));
    }

    private static URL rawURL(String resource) {
        return PulsarImporterTest.class.getResource(resource);
    }

    /**
     * Convert a CQL date to an Avro date. See rules in {@link PulsarMutationSender#cqlToAvro}
     */
    private LocalDate cqlSimpleDateToLocalDate(int value) {
        long timeInMillis = Duration.ofDays(value + Integer.MIN_VALUE).toMillis();
        Instant instant = Instant.ofEpochMilli(timeInMillis);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
    }
}
