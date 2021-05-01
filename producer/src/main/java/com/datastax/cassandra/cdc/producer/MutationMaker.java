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
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorTaskException;
import io.debezium.function.BlockingConsumer;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.UUID;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
@Slf4j
public class MutationMaker<T> {
    private final boolean emitTombstoneOnDelete;

    public MutationMaker() {
        this.emitTombstoneOnDelete = ProducerConfig.emitTombstoneOnDelete;
    }

    public void insert(String cluster, UUID node, CommitLogPosition offsetPosition,
                       String keyspace, String name, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, BlockingConsumer<Mutation<T>> consumer,
                       String md5Digest, T t) {
        createRecord(cluster, node, offsetPosition, keyspace, name, snapshot, tsMicro,
                data, markOffset, consumer, md5Digest, t);
    }

    public void update(String cluster, UUID node, CommitLogPosition offsetPosition,
                       String keyspace, String name, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, BlockingConsumer<Mutation<T>> consumer,
                       String md5Digest, T t) {
        createRecord(cluster, node, offsetPosition, keyspace, name, snapshot, tsMicro,
                data, markOffset, consumer, md5Digest, t);
    }

    public void delete(String cluster, UUID node, CommitLogPosition offsetPosition,
                       String keyspace, String name, boolean snapshot,
                       Instant tsMicro, RowData data,
                       boolean markOffset, BlockingConsumer<Mutation<T>> consumer,
                       String md5Digest, T t) {
        createRecord(cluster, node, offsetPosition, keyspace, name, snapshot, tsMicro,
                data, markOffset, consumer, md5Digest, t);
    }

    private void createRecord(String cluster, UUID node, CommitLogPosition offsetPosition,
                              String keyspace, String name, boolean snapshot,
                              Instant tsMicro, RowData data,
                              boolean markOffset, BlockingConsumer<Mutation<T>> consumer,
                              String md5Digest, T t) {
        // TODO: filter columns
        RowData filteredData = data;

        SourceInfo source = new SourceInfo(cluster, node, offsetPosition, keyspace, name, tsMicro);
        Mutation<T> record = new Mutation<T>(offsetPosition, source, filteredData, markOffset, tsMicro.toEpochMilli(), md5Digest, t);
        try {
            consumer.accept(record);
        }
        catch (InterruptedException e) {
            log.error("Interruption while enqueuing Change Event {}", record.toString());
            throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
        }
    }

}
