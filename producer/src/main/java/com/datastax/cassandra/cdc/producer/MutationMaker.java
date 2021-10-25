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
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for generating ChangeRecord and/or TombstoneRecord for create/update/delete events, as well as EOF events.
 */
@Slf4j
public class MutationMaker<T> {
    ProducerConfig config;

    public MutationMaker(ProducerConfig config) {
        this.config = config;
    }

    public void insert(UUID node, long segment, int position,
                       long tsMicro, RowData data, BlockingConsumer<Mutation<T>> consumer,
                       String md5Digest, T t, Object token) {
        createRecord(node, segment, position, tsMicro, data, consumer, md5Digest, t, token);
    }

    public void update(UUID node, long segment, int position,
                       long tsMicro, RowData data, BlockingConsumer<Mutation<T>> consumer,
                       String md5Digest, T t, Object token) {
        createRecord(node, segment, position, tsMicro, data, consumer, md5Digest, t, token);
    }

    public void delete(UUID node, long segment, int position,
                       long tsMicro, RowData data, BlockingConsumer<Mutation<T>> consumer,
                       String md5Digest, T t, Object token) {
        createRecord(node, segment, position, tsMicro,
                data, consumer, md5Digest, t, token);
    }

    private void createRecord(UUID nodeId, long segment, int position,
                              long tsMicro, RowData data, BlockingConsumer<Mutation<T>> consumer,
                              String md5Digest, T t, Object token) {
        // TODO: filter columns
        RowData filteredData = data;

        Mutation<T> record = new Mutation<T>(nodeId, segment, position, filteredData, tsMicro, md5Digest, t, token);
        try {
            consumer.accept(record);
        }
        catch (InterruptedException e) {
            log.error("Interruption while enqueuing Change Event {}", record.toString());
            throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
        }
    }
}
