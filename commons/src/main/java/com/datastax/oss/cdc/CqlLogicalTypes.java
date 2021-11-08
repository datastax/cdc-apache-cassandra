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

import org.apache.avro.*;
import org.apache.avro.generic.IndexedRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Defines customs CQL logical types schemas.
 */
public class CqlLogicalTypes {
    public static final Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema timestampMillisType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema timeMicrosType = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema uuidType = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    public static final String CQL_VARINT = "cql_varint";
    public static final CqlVarintLogicalType CQL_VARINT_LOGICAL_TYPE = new CqlVarintLogicalType();
    public static final Schema varintType  = CQL_VARINT_LOGICAL_TYPE.addToSchema(Schema.create(Schema.Type.BYTES));

    public static final String CQL_DECIMAL = "cql_decimal";
    public static final String CQL_DECIMAL_BIGINT = "bigint";
    public static final String CQL_DECIMAL_SCALE = "scale";
    public static final CqlDecimalLogicalType CQL_DECIMAL_LOGICAL_TYPE = new CqlDecimalLogicalType();
    public static final Schema decimalType  = CQL_DECIMAL_LOGICAL_TYPE.addToSchema(
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
    public static final CqlDurationLogicalType CQL_DURATION_LOGICAL_TYPE = new CqlDurationLogicalType();
    public static final Schema durationType  = CQL_DURATION_LOGICAL_TYPE.addToSchema(
            SchemaBuilder.record(CQL_DURATION)
                    .fields()
                    .name(CQL_DURATION_MONTHS).type().intType().noDefault()
                    .name(CQL_DURATION_DAYS).type().intType().noDefault()
                    .name(CQL_DURATION_NANOSECONDS).type().longType().noDefault()
                    .endRecord()
    );

    public static class CqlVarintLogicalType extends LogicalType {
        public CqlVarintLogicalType() {
            super(CQL_VARINT);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            // validate the type
            if (schema.getType() != Schema.Type.BYTES) {
                throw new IllegalArgumentException("Logical type cql_varint must be backed by bytes");
            }
        }
    }

    public static class CqlDecimalLogicalType extends LogicalType {
        public CqlDecimalLogicalType() {
            super(CQL_DECIMAL);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            // validate the type
            if (schema.getType() != Schema.Type.RECORD) {
                throw new IllegalArgumentException("Logical type cql_decimal must be backed by a record");
            }
        }
    }

    public static class CqlDurationLogicalType extends LogicalType {
        public CqlDurationLogicalType() {
            super(CQL_DURATION);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            // validate the type
            if (schema.getType() != Schema.Type.RECORD) {
                throw new IllegalArgumentException("Logical type cql_duration must be backed by a record");
            }
        }
    }

    public static class CqlVarintConversion extends Conversion<BigInteger> {
        @Override
        public Class<BigInteger> getConvertedType() {
            return BigInteger.class;
        }

        @Override
        public String getLogicalTypeName() {
            return CQL_VARINT;
        }

        @Override
        public BigInteger fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
            byte[] arr = new byte[value.remaining()];
            value.duplicate().get(arr);
            return new BigInteger(arr);
        }

        @Override
        public ByteBuffer toBytes(BigInteger value, Schema schema, LogicalType type) {
            return ByteBuffer.wrap(value.toByteArray());
        }
    }

    public static class CqlDecimalConversion extends Conversion<BigDecimal> {
        @Override
        public Class<BigDecimal> getConvertedType() {
            return BigDecimal.class;
        }

        @Override
        public String getLogicalTypeName() {
            return CQL_DECIMAL;
        }

        @Override
        public BigDecimal fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
            ByteBuffer bb = (ByteBuffer) value.get(0);
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            int scale = (int) value.get(1);
            return new BigDecimal(new BigInteger(bytes), scale);
        }

        @Override
        public IndexedRecord toRecord(BigDecimal value, Schema schema, LogicalType type) {
            return new org.apache.avro.generic.GenericRecordBuilder(CqlLogicalTypes.decimalType)
                    .set(CqlLogicalTypes.CQL_DECIMAL_BIGINT, ByteBuffer.wrap(value.unscaledValue().toByteArray()))
                    .set(CqlLogicalTypes.CQL_DECIMAL_SCALE, value.scale())
                    .build();
        }
    }
}
