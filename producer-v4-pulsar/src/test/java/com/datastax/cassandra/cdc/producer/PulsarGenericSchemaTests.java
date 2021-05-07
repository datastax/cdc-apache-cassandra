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
        PulsarMutationSender pulsarMutationSender = new PulsarMutationSender();

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
