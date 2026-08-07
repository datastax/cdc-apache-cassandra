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

package com.datastax.oss.cdc.backfill.importer;

import com.datastax.oss.cdc.agent.AbstractMutation;
import com.datastax.oss.cdc.agent.Mutation;
import com.datastax.oss.cdc.agent.MutationSender;
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import com.datastax.oss.cdc.backfill.ExitStatus;
import com.datastax.oss.cdc.backfill.exporter.ExportedTable;
import com.datastax.oss.cdc.backfill.factory.CodecFactory;
import com.datastax.oss.cdc.backfill.factory.ConnectorFactory;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.string.StringConvertingCodecProvider;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;

/**
 * Package-private base class shared by {@link PulsarImporter} and {@link KafkaImporter}.
 *
 * <p>Contains all import logic: CSV reading, PK codec preparation, mutation construction,
 * async send with back-pressure semaphore, and summary logging. Subclasses only provide
 * {@link #importerName()} for log lines and a constructor that supplies the sender.
 */
public abstract class AbstractImporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractImporter.class);

    final ConnectorFactory connectorFactory;
    final ExportedTable exportedTable;
    final MutationSender<TableMetadata> mutationSender;
    final Semaphore inflightMessages;

    /** Keeps track of last mutation future exception to facilitate a fail-fast strategy. */
    volatile Throwable lastException = null;

    /**
     * Token is not used for CDC back-fill — round-robin routing is used instead.
     */
    private static final String MUTATION_TOKEN = "";

    /**
     * Commit log segment id and position — not applicable for CDC back-filling.
     */
    private static final long MUTATION_SEGMENT = -1;
    private static final int MUTATION_OFFSET = -1;

    /**
     * Used for deduplication when mutations are sent from the agents. The back-fill CLI cannot
     * compute the real digest, but a constant value is sufficient to mimic an insert, and we don't
     * expect dedupe to kick in because the CLI processes each mutation once.
     */
    private static final String MUTATION_DIGEST = "BACK_FILL_INSERT";

    /**
     * Coordinator node — not applicable for CDC back-filling.
     */
    private static final UUID MUTATION_NODE = null;

    private static final ConvertingCodecFactory codecFactory =
            new CodecFactory().newCodecFactory(AbstractImporter.class.getClassLoader());

    /** The maximum number of in-flight messages currently being imported. */
    @VisibleForTesting
    public static final int MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING = 1000;

    final AtomicInteger sentMutations = new AtomicInteger(0);
    final AtomicInteger sentErrors = new AtomicInteger(0);

    AbstractImporter(ConnectorFactory connectorFactory, ExportedTable exportedTable,
                     MutationSender<TableMetadata> mutationSender) {
        this.connectorFactory = connectorFactory;
        this.exportedTable = exportedTable;
        this.mutationSender = mutationSender;
        this.inflightMessages = new Semaphore(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING);
    }

    /** Returns the importer name used in log messages, e.g. {@code "Pulsar"} or {@code "Kafka"}. */
    protected abstract String importerName();

    @SuppressWarnings("unchecked")
    public ExitStatus importTable() {
        Connector connector = null;
        long recordsCount = -1;
        try {
            connector = connectorFactory.newCVSConnector();
            // Explicitly request a string codec provider to avoid class loader unaware issues at runtime
            StringConvertingCodecProvider stringConvertingCodecProvider = new StringConvertingCodecProvider();
            Map<String, ConvertingCodec<String, AbstractType<?>>> codecs =
                    this.exportedTable.getPrimaryKey()
                            .stream()
                            .map(k -> {
                                Optional<ConvertingCodec<?, ?>> codec =
                                        stringConvertingCodecProvider.maybeProvide(k.getType(), GenericType.STRING, codecFactory, false);
                                if (!codec.isPresent()) {
                                    throw new RuntimeException("Codec not found for requested operation: ["
                                            + k.getType() + " <-> java.lang.String]");
                                }
                                return new AbstractMap.SimpleEntry<>(
                                        k.getName().toString(),
                                        (ConvertingCodec<String, AbstractType<?>>) codec.get());
                            })
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            List<DefaultMappedField> fields = this.exportedTable
                    .getPrimaryKey()
                    .stream()
                    .map(ColumnMetadata::getName)
                    .map(Object::toString)
                    .map(DefaultMappedField::new)
                    .collect(Collectors.toList());

            recordsCount = Flux
                    .from(connector.read())
                    .flatMap(Resource::read).map(record -> {
                        List<Object> pkValues = fields.stream().map(field -> {
                            Object val = record.getFieldValue(field);
                            Object newVal = codecs.get(field.getFieldName()).externalToInternal((String) val);
                            if (newVal instanceof LocalTime) {
                                // Agent expects TimeType to be Long in nanoseconds
                                newVal = ((LocalTime) newVal).toNanoOfDay();
                            } else if (newVal instanceof LocalDate) {
                                // Agent expects SimpleDateType to be Integer in epoch days
                                newVal = SimpleDateSerializer.timeInMillisToDay(
                                        ((LocalDate) newVal).atStartOfDay(UTC).toInstant().toEpochMilli());
                            }
                            return newVal;
                        }).collect(Collectors.toList());
                        // Disables the e2e latency metric because the writetime property won't be set
                        final long tsMicro = -1;
                        final AbstractMutation<TableMetadata> mutation =
                                createMutation(pkValues.toArray(), this.exportedTable.getCassandraTable(), tsMicro);
                        sendMutationAsync(mutation);
                        return record;
                    })
                    .takeWhile(resource -> lastException == null) // fail fast
                    .count()
                    .block();

            if (lastException != null) {
                return ExitStatus.STATUS_ABORTED_FATAL_ERROR;
            }

            // Acquire all permits to ensure all in-flight messages have finished processing
            inflightMessages.acquire(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING);

            // An error could've happened in the last batch, re-check
            if (lastException != null) {
                return ExitStatus.STATUS_ABORTED_FATAL_ERROR;
            }

            return ExitStatus.STATUS_OK;
        } catch (Exception e) {
            lastException = e;
            return ExitStatus.STATUS_ABORTED_FATAL_ERROR;
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (Exception e) {
                    LOGGER.warn("Error while closing CSV connector", e);
                }
            }
            if (mutationSender instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) mutationSender).close();
                } catch (Exception e) {
                    LOGGER.warn("Error while closing {} mutation sender", importerName(), e);
                }
            }
            printSummary(recordsCount);
        }
    }

    private AbstractMutation<TableMetadata> createMutation(Object[] pkValues, TableMetadata tableMetadata, long tsMicro) {
        return new Mutation(MUTATION_NODE, MUTATION_SEGMENT, MUTATION_OFFSET,
                pkValues, tsMicro, MUTATION_DIGEST, tableMetadata, MUTATION_TOKEN);
    }

    private void printSummary(long recordsCount) {
        ExitStatus status = ExitStatus.STATUS_OK;
        if (lastException != null) {
            LOGGER.error("Failed to import table", lastException);
            status = ExitStatus.STATUS_ABORTED_FATAL_ERROR;
        }
        LOGGER.info("{} Importer Summary: Import status={}, " +
                        "Read mutations from disk={}, Sent mutations={}, Failed mutations={}",
                importerName(), status, recordsCount, sentMutations.get(), sentErrors.get());
    }

    private void sendMutationAsync(AbstractMutation<TableMetadata> mutation) {
        LOGGER.debug("Sending mutation={}", mutation);
        try {
            inflightMessages.acquireUninterruptibly(); // may block
            this.mutationSender.sendMutationAsync(mutation)
                    .handle((msgId, e) -> {
                        try {
                            if (e == null) {
                                sentMutations.incrementAndGet();
                                LOGGER.debug("Sent mutation={}", mutation);
                            } else {
                                if (e instanceof CassandraConnectorSchemaException) {
                                    LOGGER.error("Invalid primary key schema for mutation={}", mutation);
                                } else {
                                    LOGGER.error("Send failed for mutation={}", mutation);
                                }
                                sentErrors.incrementAndGet();
                                lastException = e;
                            }
                            return msgId;
                        } finally {
                            inflightMessages.release();
                        }
                    });
        } catch (Exception e) {
            LOGGER.error("Send failed:", e);
            sentErrors.incrementAndGet();
            throw e;
        }
    }
}
