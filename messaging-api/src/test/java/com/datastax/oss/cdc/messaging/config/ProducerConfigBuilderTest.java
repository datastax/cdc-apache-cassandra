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
package com.datastax.oss.cdc.messaging.config;

import com.datastax.oss.cdc.messaging.config.impl.ProducerConfigBuilder;
import com.datastax.oss.cdc.messaging.schema.SchemaType;
import com.datastax.oss.cdc.messaging.schema.impl.BaseSchemaDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProducerConfigBuilderTest {

    private static ProducerConfigBuilder<byte[], byte[]> minimalBuilder() {
        return ProducerConfigBuilder.<byte[], byte[]>builder()
                .topic("events-ks.table")
                .keySchema(BaseSchemaDefinition.builder().type(SchemaType.AVRO)
                        .schemaDefinition("\"string\"").name("key").build())
                .valueSchema(BaseSchemaDefinition.builder().type(SchemaType.AVRO)
                        .schemaDefinition("\"string\"").name("value").build());
    }

    @Test
    public void zeroSendTimeoutMeansInfiniteAndIsAccepted() {
        ProducerConfig<byte[], byte[]> config = minimalBuilder().sendTimeoutMs(0).build();
        assertEquals(0, config.getSendTimeoutMs(),
                "0 must be allowed (no timeout / infinite, Pulsar backward-compatible default)");
    }

    @Test
    public void positiveSendTimeoutIsAccepted() {
        ProducerConfig<byte[], byte[]> config = minimalBuilder().sendTimeoutMs(5000).build();
        assertEquals(5000, config.getSendTimeoutMs());
    }

    @Test
    public void negativeSendTimeoutIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> minimalBuilder().sendTimeoutMs(-1));
    }

    @Test
    public void buildRequiresTopicAndSchemas() {
        assertThrows(IllegalStateException.class,
                () -> ProducerConfigBuilder.<byte[], byte[]>builder().build());
    }
}
