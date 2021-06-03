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

import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;


public class PulsarGenericSchemaTests {

    @Test
    public void testGenericSchema() {
        ProducerConfig config = ProducerConfig.create(ProducerConfig.Plateform.PULSAR, null);
        PulsarMutationSender pulsarMutationSender = new PulsarMutationSender(config);

        RecordSchemaBuilder schemaBuilder = SchemaBuilder.record("testrecord");
        int i = 0;
        for (SchemaType type : pulsarMutationSender.schemaTypes.values()) {
            schemaBuilder
                    .field("a"+i)
                    .type(type);
            i++;
        }
        SchemaInfo schemaInfo = schemaBuilder.build(SchemaType.AVRO);
        GenericSchemaImpl.of(schemaInfo);
    }
}
