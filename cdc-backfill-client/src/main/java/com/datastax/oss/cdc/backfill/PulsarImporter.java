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
import com.datastax.oss.cdc.agent.Mutation;
import com.datastax.oss.cdc.agent.PulsarMutationSender;

import com.datastax.oss.cdc.backfill.factory.PulsarMutationSenderFactory;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class PulsarImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarImporter.class);

    final private Connector connector;
    final private TableMetadata tableMetadata;

    private final PulsarMutationSender mutationSender;

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

    public PulsarImporter(Connector connector, TableMetadata tableMetadata, PulsarMutationSenderFactory factory) {
        this.connector = connector;
        this.tableMetadata = tableMetadata;
        this.mutationSender = factory.newPulsarMutationSender();
    }

    public ExitStatus importTable() {
        try {
            List<CompletableFuture<?>> futures = new ArrayList<>();
            List<DefaultMappedField> fields = new ArrayList<>();
            this.tableMetadata.primaryKeyColumns().forEach(f-> fields.add(new DefaultMappedField(f.toString())));
            long c = Flux
                    .from(connector.read())
                    .flatMap(Resource::read).map(record -> {
                        // TODO: Convert CVS values (string) to Avro types
                        Object[] pkValues = fields.stream().map(record::getFieldValue).toArray();
                        // tsMicro is used to emit e2e metrics by the connectors, if you carry over the C* WRITETIME
                        // of the source records, the metric will be greatly skewed because those records are historical.
                        // For now, will mimic the metric by using now()
                        // TODO: Disable the e2e latency metric if the records are emitted from cdc back-filling CLI
                        final long tsMicro = Instant.now().toEpochMilli() * 1000;
                        futures.add(mutationSender.sendMutationAsync(createMutation(pkValues, tableMetadata, tsMicro)));
                        return record;
                    })
                    .count()
                    .block();

            LOGGER.info("Sent {} records to Pulsar", c);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
            return ExitStatus.STATUS_OK;

        } catch (Exception e) {
            LOGGER.error("Failed to import table", e);
            return ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (Exception e) {
                    LOGGER.warn("Error while closing CVS connector", e);
                }
            }
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

}
