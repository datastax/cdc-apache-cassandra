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
package com.datastax.oss.pulsar.source.converters;

import com.datastax.oss.cdc.NativeSchemaWrapper;
import com.datastax.oss.cdc.converters.JsonRowConverter;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.pulsar.source.Converter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;

public class PulsarJsonConverter extends JsonRowConverter implements Converter<byte[], GenericRecord, Row, byte[]> {
    public final org.apache.pulsar.client.api.Schema<byte[]> pulsarSchema;

    public PulsarJsonConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        super(ksm, tm, columns);
        this.pulsarSchema = new NativeSchemaWrapper(nativeSchema, SchemaType.JSON);
    }

    @Override
    public org.apache.pulsar.client.api.Schema<byte[]> getSchema() {
        return this.pulsarSchema;
    }
}
