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

import com.datastax.oss.cdc.converters.AvroRowConverter;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

/**
 * Unlike {@code PulsarAvroConverter}, this class adds no platform-specific state on top of
 * {@link AvroRowConverter}: Kafka Connect's {@code SourceRecord} takes the raw Avro bytes
 * directly under {@code Schema.BYTES_SCHEMA} (see {@code KafkaCassandraSourceTask#buildSourceRecord}),
 * with no equivalent of Pulsar's {@code Schema<byte[]>}/{@code NativeSchemaWrapper} needed to
 * hand the bytes to the client API. The subclass exists so {@code Converter.class} tokens can
 * select Avro vs. JSON encoding, and to optionally publish through a Confluent Schema Registry.
 *
 * <p>{@link #enableSchemaRegistry} is a setter rather than a constructor argument because every
 * converter (Pulsar and Kafka alike) is instantiated reflectively through the shared, platform-free
 * {@code ConverterFactory} with a fixed {@code (KeyspaceMetadata, TableMetadata, List<ColumnMetadata>)}
 * constructor; {@code KafkaCassandraSourceTask} calls it right after construction instead.
 */
public class KafkaAvroConverter extends AvroRowConverter implements Converter<byte[], List<Object>> {

    private volatile KafkaAvroSerializer schemaRegistrySerializer;
    private volatile String outputTopic;

    public KafkaAvroConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        super(ksm, tm, columns);
    }

    /**
     * Switches this converter to publish through a Confluent Schema Registry: {@code serializer}
     * registers {@link #nativeSchema} under the {@code <outputTopic>-value} subject (subject to
     * the registry's own compatibility mode) and prepends the Confluent wire-format header
     * (magic byte + 4-byte schema id) instead of writing raw Avro bytes.
     */
    public void enableSchemaRegistry(KafkaAvroSerializer serializer, String outputTopic) {
        this.schemaRegistrySerializer = serializer;
        this.outputTopic = outputTopic;
    }

    @Override
    public byte[] toConnectData(Row row) {
        GenericRecord record = buildGenericRecord(row);
        return schemaRegistrySerializer != null
                ? schemaRegistrySerializer.serialize(outputTopic, record)
                : serializeAvroGenericRecord(record, nativeSchema);
    }
}
