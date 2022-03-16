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
