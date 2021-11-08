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

import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorTaskException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.schema.TableMetadata;

import java.util.UUID;

@Slf4j
public class MutationMaker extends AbstractMutationMaker<TableMetadata> {

    public MutationMaker(AgentConfig config) {
        super(config);
    }

    public void createRecord(UUID nodeId, long segment, int position,
                              long tsMicro, Object[] pkValues, BlockingConsumer<AbstractMutation<TableMetadata>> consumer,
                              String md5Digest, TableMetadata t, Object token) {
        Mutation record = new Mutation(nodeId, segment, position, pkValues, tsMicro, md5Digest, t, token);
        try {
            consumer.accept(record);
        }
        catch (InterruptedException e) {
            log.error("Interruption while enqueuing Change Event {}", record);
            throw new CassandraConnectorTaskException("Enqueuing has been interrupted: ", e);
        }
    }
}
