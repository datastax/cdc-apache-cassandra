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
package com.datastax.oss.cdc;

import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;

public class AvroSchemaWrapper implements org.apache.pulsar.client.api.Schema<byte[]> {

    private final SchemaInfo schemaInfo;
    private final Schema nativeAvroSchema;

    public AvroSchemaWrapper(Schema nativeAvroSchema) {
        this.nativeAvroSchema = nativeAvroSchema;
        this.schemaInfo = SchemaInfoImpl.builder()
                .schema(nativeAvroSchema.toString(false).getBytes(StandardCharsets.UTF_8))
                .properties(new HashMap<>())
                .type(SchemaType.AVRO)
                .name(nativeAvroSchema.getName())
                .build();
    }

    @Override
    public byte[] encode(byte[] bytes) {
        return bytes;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public AvroSchemaWrapper clone() {
        return new AvroSchemaWrapper(nativeAvroSchema);
    }

    @Override
    public void validate(byte[] message) {
        // nothing to do
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {

    }

    @Override
    public byte[] decode(byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] decode(byte[] bytes, byte[] schemaVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return true;
    }

    @Override
    public void configureSchemaInfo(String topic, String componentName, SchemaInfo schemaInfo) {

    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(nativeAvroSchema);
    }
}
