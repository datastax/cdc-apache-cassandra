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

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;

public class AvroGenericRecord implements GenericRecord {
    List<Field> fields;
    org.apache.avro.generic.GenericRecord genericRecord;

    public AvroGenericRecord(List<Field> fields, org.apache.avro.generic.GenericRecord genericRecord) {
        this.fields = fields;
        this.genericRecord = genericRecord;
    }

    @Override
    public byte[] getSchemaVersion() {
        return new byte[0];
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    @Override
    public Object getField(String s) {
        return genericRecord.get(s);
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.AVRO;
    }

    @Override
    public Object getNativeObject() {
        return genericRecord;
    }
}
