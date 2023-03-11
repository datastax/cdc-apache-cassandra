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
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import com.datastax.oss.cdc.backfill.ExitStatus;
import com.datastax.oss.cdc.backfill.exporter.ExportedTable;
import com.datastax.oss.cdc.backfill.factory.ConnectorFactory;
import com.datastax.oss.cdc.backfill.factory.PulsarMutationSenderFactory;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.string.StringConvertingCodecProvider;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;

public class PulsarImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarImporter.class);

    final private ConnectorFactory connectorFactory;
    final private ExportedTable exportedTable;

    private final PulsarMutationSender mutationSender;

    private final Semaphore inflightPulsarMessages;

    /**
     * Keeps track of last mutation future exception to facilitate a fail-fast strategy
     */
    private volatile Throwable lastException = null;

    /**
     *  Token is not used because for CDC back-fill purposes, we used a round-robin routing mode when sending
     *  mutations. In the regular CDC operations mode, the routing algorithm follows what the C* partitioner use
     *  as per {@link DatabaseDescriptor#getPartitionerName()}
     */
    private final static String MUTATION_TOKEN = "";

    /**
     *  Commit log segment id and position message property names that originates from the commit log and are
     *  used for e2e testing. Doesn't apply for CDC back-filling.
     */
    private final static long MUTATION_SEGMENT = -1;
    private final static int MUTATION_OFFSET = -1;

    /**
     * Used for deduplication when mutations are sent from the agents. Please note that the digest is calculated
     * based on the {@link org.apache.cassandra.db.Mutation} and not the wrapper mutation object
     * {@link AbstractMutation} which makes it impossible for the back-filling CLI to calculate.
     * However, reusing the same constant for the digest would suffice to mimic and insert, and we don't expect
     * dedupe to kick in because the CLI tool will process each mutation once.
     */
    private final static String MUTATION_DIGEST = "BACK_FILL_INSERT";

    /**
     * Used by the connector to explicitly set the coordinator node to that once that originally comes form the agent
     * node. Doesn't apply for CDC back-filling.
     */
    private final static UUID MUTATION_NODE = null;
    private final static ConvertingCodecFactory codecFactory = new ConvertingCodecFactory();

    /**
     * The maximum number of in-flight pulsar messages currently being imported
     */
    @VisibleForTesting
    public static final int MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING = 1000;

    private final AtomicInteger sentMutations = new AtomicInteger(0);
    private final AtomicInteger sentErrors = new AtomicInteger(0);

    public PulsarImporter(ConnectorFactory connectorFactory, ExportedTable exportedTable, PulsarMutationSenderFactory factory) {
        this.connectorFactory = connectorFactory;
        this.exportedTable = exportedTable;
        this.mutationSender = factory.newPulsarMutationSender();
        this.inflightPulsarMessages = new Semaphore(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING);
    }

    @SuppressWarnings("unchecked")
    public ExitStatus importTable() {
        Connector connector = null;
        long recordsCount = -1;
        try {
            connector = connectorFactory.newCVSConnector();
            // prepare PK codecs
            // Explicitly request a string codec provider to avoid class loader unware issues at runtime
            StringConvertingCodecProvider stringConvertingCodecProvider = new StringConvertingCodecProvider();
            Map<String, ConvertingCodec<String, AbstractType<?>>> codecs =
                    this.exportedTable.getPrimaryKey()
                            .stream()
                            .map(k-> {
                                Optional<ConvertingCodec<?, ?>> codec =
                                        stringConvertingCodecProvider.maybeProvide(k.getType(), GenericType.STRING, codecFactory, false);
                                if (!codec.isPresent()) {
                                    throw new RuntimeException("Codec not found for requested operation: ["
                                            + k.getType() + " <-> java.lang.String]");
                                }
                                return new AbstractMap.SimpleEntry<>(
                                    k.getName().toString(),
                                    (ConvertingCodec<String, AbstractType<?>>) codec.get());
                            } )
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // prepare fields
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
                        List<Object> pkValues = fields.stream().map(field-> {
                            Object val = record.getFieldValue(field);
                            Object newVal = codecs.get(field.getFieldName()).externalToInternal((String) val);
                            if (newVal instanceof LocalTime) {
                                // Agent expect TimeType to be Long in nanoseconds
                                // see com.datastax.oss.cdc.agent.PulsarMutationSender#cqlToAvro
                                newVal = ((LocalTime) newVal).toNanoOfDay();
                            } else if (newVal instanceof LocalDate) {
                                // Agent expect SimpleDateType to be Integer in epoch days
                                // see com.datastax.oss.cdc.agent.PulsarMutationSender#cqlToAvro
                                newVal = SimpleDateSerializer.timeInMillisToDay(
                                        ((LocalDate) newVal).atStartOfDay(UTC).toInstant().toEpochMilli());
                            }
                            return newVal;
                        }).collect(Collectors.toList());
                        // tsMicro is used to emit e2e metrics by the connectors, if you carry over the C* WRITETIME
                        // of the source records, the metric will be greatly skewed because those records are historical.
                        // For now, will mimic the metric by using now()
                        // TODO: Disable the e2e latency metric if the records are emitted from cdc back-filling CLI
                        final long tsMicro = Instant.now().toEpochMilli() * 1000;
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

            // Attempt to acquire all the permits to ensure that all the messages have finished processing
            inflightPulsarMessages.acquire(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING);

            // An error could've happened in the last batch of messages, re-check last exception
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
                    LOGGER.warn("Error while closing CVS connector", e);
                }
            }
            printSummary(recordsCount);
        }
    }

    private AbstractMutation<TableMetadata> createMutation(Object[] pkValues, TableMetadata tableMetadata, long tsMicro) {
        return new Mutation(MUTATION_NODE,
                MUTATION_SEGMENT,
                MUTATION_OFFSET,
                pkValues, tsMicro,
                MUTATION_DIGEST,
                tableMetadata,
                MUTATION_TOKEN);
    }

    private void printSummary(long recordsCount) {
        ExitStatus status = ExitStatus.STATUS_OK;
        if (lastException != null) {
            LOGGER.error("Failed to import table", lastException);
            status = ExitStatus.STATUS_ABORTED_FATAL_ERROR;
        }
        LOGGER.info("Pulsar Importer Summary: Import status={}, " +
                        "Read mutations from disk={}, Sent mutations={}, Failed mutations={}", status,
                recordsCount, sentMutations.get(), sentErrors.get());
    }

    private void sendMutationAsync(AbstractMutation<TableMetadata> mutation) {
        LOGGER.debug("Sending mutation={}", mutation);
        try {
            inflightPulsarMessages.acquireUninterruptibly(); // may block
            this.mutationSender.sendMutationAsync(mutation)
                .handle((msgId, e)-> {
                    try {
                        if (e == null) {
                            sentMutations.incrementAndGet();
                            LOGGER.debug("Sent mutation={}", mutation);
                        } else {
                            if (e instanceof CassandraConnectorSchemaException) {
                                LOGGER.error("Invalid primary key schema for mutation={}", mutation);
                            } else {
                                LOGGER.error("Sent failed mutation={}", mutation);
                            }
                            sentErrors.incrementAndGet();
                            lastException = e;
                        }
                        return msgId;
                    } finally {
                        inflightPulsarMessages.release();
                    }
                });
        } catch(Exception e) {
            LOGGER.error("Send failed:", e);
            sentErrors.incrementAndGet();
            throw e;
        }
    }
}
