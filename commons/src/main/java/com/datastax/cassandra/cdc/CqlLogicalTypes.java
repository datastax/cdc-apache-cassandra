package com.datastax.cassandra.cdc;


import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Defines customs CQL logical types schemas.
 */
public class CqlLogicalTypes {
    public static final Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema timestampMillisType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema timestampMicrosType = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema timeMillisType = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema timeMicrosType = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema uuidType = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    public static final Schema varintType  = new LogicalType("cql_varint").addToSchema(
            Schema.create(Schema.Type.BYTES)
    );

    public static final String CQL_DECIMAL = "cql_decimal";
    public static final String CQL_DECIMAL_BIGINT = "bigint";
    public static final String CQL_DECIMAL_SCALE = "scale";

    public static final Schema decimalType  = new LogicalType(CQL_DECIMAL).addToSchema(
            SchemaBuilder.record(CQL_DECIMAL)
                    .fields()
                    .name(CQL_DECIMAL_BIGINT).type().bytesType().noDefault()
                    .name(CQL_DECIMAL_SCALE).type().intType().noDefault()
                    .endRecord()
    );

    public static final String CQL_DURATION = "cql_duration";
    public static final String CQL_DURATION_MONTHS = "months";
    public static final String CQL_DURATION_DAYS = "days";
    public static final String CQL_DURATION_NANOSECONDS = "nanoseconds";

    public static final Schema durationType  = new LogicalType(CQL_DURATION).addToSchema(
            SchemaBuilder.record(CQL_DURATION)
                    .fields()
                    .name(CQL_DURATION_MONTHS).type().intType().noDefault()
                    .name(CQL_DURATION_DAYS).type().intType().noDefault()
                    .name(CQL_DURATION_NANOSECONDS).type().longType().noDefault()
                    .endRecord()
    );
}
