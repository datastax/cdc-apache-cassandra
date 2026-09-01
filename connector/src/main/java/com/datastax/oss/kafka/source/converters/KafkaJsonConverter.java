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
package com.datastax.oss.kafka.source.converters;

import com.datastax.oss.cdc.converters.JsonRowConverter;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.util.List;

/**
 * Unlike {@code PulsarJsonConverter}, this class adds no platform-specific state on top of
 * {@link JsonRowConverter}: Kafka Connect's {@code SourceRecord} takes the raw JSON bytes
 * directly under {@code Schema.BYTES_SCHEMA} (see {@code KafkaCassandraSourceTask#buildSourceRecord}),
 * with no equivalent of Pulsar's {@code Schema<byte[]>}/{@code NativeSchemaWrapper} needed to
 * hand the bytes to the client API. The subclass exists only so {@code Converter.class} tokens
 * can select Avro vs. JSON encoding.
 */
public class KafkaJsonConverter extends JsonRowConverter implements Converter<byte[], byte[]> {

    public KafkaJsonConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        super(ksm, tm, columns);
    }
}
