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

import com.clearspring.analytics.stream.membership.DataOutputBuffer;
import com.datastax.oss.cdc.agent.AgentConfig;
import com.datastax.oss.cdc.agent.Mutation;
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.datastax.oss.cdc.agent.AgentConfig.PULSAR_SERVICE_URL;

public class PulsarImporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableExporter.class);

    final private Connector connector;
    final private ExportedTable exportedTable;

    public PulsarImporter(Connector connector, ExportedTable exportedTable) {
        this.connector = connector;
        this.exportedTable = exportedTable;
    }

    public TableExportReport importTable(TableExporter migrator) {
        try {
            connector.init();
        } catch (Exception e) {
            throw new RuntimeException("Failed to init connector!", e);
        }
        String operationId = "OperationID"; // TODO: Retrieve operation ID retrieveImportOperationId();
        Map<String, Object> tenantInfo = new HashMap<>();
        tenantInfo.put(PULSAR_SERVICE_URL, "pulsar://localhost:6650/");
        //tenantInfo.put(PULSAR_AUTH_PLUGIN_CLASS_NAME, "MyAuthPlugin");
        //tenantInfo.put(PULSAR_AUTH_PARAMS, "sdds");
        //tenantInfo.put(SSL_ALLOW_INSECURE_CONNECTION, "true");
        //tenantInfo.put(SSL_HOSTNAME_VERIFICATION_ENABLE, "true");
        //tenantInfo.put(TLS_TRUST_CERTS_FILE_PATH, "/test.p12");

        AgentConfig config = AgentConfig.create(AgentConfig.Platform.PULSAR, tenantInfo);
        PulsarMutationSender sender = new PulsarMutationSender(config, true);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        long c = Flux.from(connector.read()).flatMap(Resource::read).map(record -> {
            //System.out.println("Sending to pulsar " + record.toString());
            Object[] pkValues = new Object[]{record.getFieldValue(new DefaultIndexedField(0))};
            DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
            //org.apache.cassandra.db.Mutation.serializer.serialize(mutation, dataOutputBuffer, descriptor.getMessagingVersion());
            //String md5Digest = DigestUtils.md5Hex(dataOutputBuffer.getData());
            TableMetadata.Builder builder = TableMetadata.builder(exportedTable.keyspace.getName().toString(), exportedTable.table.getName().toString());
            exportedTable.table.getPartitionKey().forEach(k-> builder.addPartitionKeyColumn(k.getName().toString(), UTF8Type.instance));
            TableMetadata t = builder.build();
            futures.add(sender.sendMutationAsync(new Mutation(UUID.randomUUID(), 0L, 0, pkValues, 0, "", t, "")));
            return record;
        }).count().block().longValue();

        LOGGER.info("sent {} records to Pulsar", c);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
        return new TableExportReport(migrator, ExitStatus.STATUS_OK, operationId, true);
    }

}
