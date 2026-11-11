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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.util.UUID;

/**
 * Kafka mutation sender for Apache Cassandra 4.0.
 * Type mapping and value conversion live in {@link CqlAvroTypes}; this class only supplies the
 * Cassandra version specific lookups.
 */
public class KafkaMutationSender extends AbstractKafkaMutationSender<TableMetadata> {

    public KafkaMutationSender(AgentConfig config) {
        super(config, DatabaseDescriptor.getPartitionerName().equals(Murmur3Partitioner.class.getName()));
    }

    public KafkaMutationSender(AgentConfig config, boolean useMurmur3Partitioner) {
        super(config, useMurmur3Partitioner);
    }

    @Override
    public void incSkippedMutations() {
        CdcMetrics.skippedMutations.inc();
    }

    @Override
    public UUID getHostId() {
        return StorageService.instance.getLocalHostUUID();
    }

    @Override
    public String columnCql3Type(TableMetadata tableMetadata, String columnName) {
        return tableMetadata.getColumn(ColumnIdentifier.getInterned(columnName, false))
                .type.asCQL3Type().toString();
    }
}
