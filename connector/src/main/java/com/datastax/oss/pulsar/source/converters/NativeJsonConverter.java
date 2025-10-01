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

import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.pulsar.common.schema.SchemaType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class NativeJsonConverter extends AbstractNativeConverter<byte[]> {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);

    public NativeJsonConverter(KeyspaceMetadata ksm, TableMetadata tm, List<ColumnMetadata> columns) {
        super(ksm, tm, columns);
    }

    @Override
    SchemaType getSchemaType() {
        return SchemaType.JSON;
    }

    @Override
    public byte[] toConnectData(Row row) {
        ObjectNode node = jsonNodeFactory.objectNode();
        for(ColumnDefinition cm : row.getColumnDefinitions()) {
            String fieldName = cm.getName().toString();
            node.set(fieldName, toJson(row, cm.getName(), cm.getType()));
        }
        return serializeJsonNode(node);
    }

    JsonNode toJson(GettableById record, CqlIdentifier identifier, DataType dataType) {
        if (record.isNull(identifier)) {
            return jsonNodeFactory.nullNode();
        }
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
                return jsonNodeFactory.textNode(record.getUuid(identifier).toString());
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                return jsonNodeFactory.textNode(record.getString(identifier));
            case ProtocolConstants.DataType.TINYINT:
                return jsonNodeFactory.numberNode((int) record.getByte(identifier));
            case ProtocolConstants.DataType.SMALLINT:
                return jsonNodeFactory.numberNode((int) record.getShort(identifier));
            case ProtocolConstants.DataType.INT:
                return jsonNodeFactory.numberNode(record.getInt(identifier));
            case ProtocolConstants.DataType.BIGINT:
                return jsonNodeFactory.numberNode(record.getLong(identifier));
            case ProtocolConstants.DataType.INET:
                return jsonNodeFactory.textNode(record.getInetAddress(identifier).getHostAddress());
            case ProtocolConstants.DataType.DOUBLE:
                return jsonNodeFactory.numberNode(record.getDouble(identifier));
            case ProtocolConstants.DataType.FLOAT:
                return jsonNodeFactory.numberNode(record.getFloat(identifier));
            case ProtocolConstants.DataType.BOOLEAN:
                return jsonNodeFactory.booleanNode(record.getBoolean(identifier));
            case ProtocolConstants.DataType.TIMESTAMP:
                return jsonNodeFactory.numberNode(record.getInstant(identifier).toEpochMilli());
            case ProtocolConstants.DataType.DATE: // Mimic Avro date (epoch days)
                return jsonNodeFactory.numberNode((int) record.getLocalDate(identifier).toEpochDay());
            case ProtocolConstants.DataType.TIME: // Mimic Avro time (microseconds)
                return jsonNodeFactory.numberNode (record.getLocalTime(identifier).toNanoOfDay() / 1000);
            case ProtocolConstants.DataType.BLOB:
                return jsonNodeFactory.binaryNode(getBytes(record.getByteBuffer(identifier)));
            case ProtocolConstants.DataType.UDT:
                return buildUDTValue(record.getUdtValue(identifier));
            case ProtocolConstants.DataType.DURATION:
                return cqlDurationToJsonNode(record.getCqlDuration(identifier));
            case ProtocolConstants.DataType.DECIMAL:
                return bigDecimalToJsonNode(record.getBigDecimal(identifier));
            case ProtocolConstants.DataType.VARINT:
                Conversion<BigInteger> conversion = SpecificData.get().getConversionByClass(BigInteger.class);
                ByteBuffer bf = conversion.toBytes(record.getBigInteger(identifier), CqlLogicalTypes.varintType, CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE);
                return jsonNodeFactory.binaryNode(getBytes(bf));
            case ProtocolConstants.DataType.LIST: {
                ListType listType = (ListType) dataType;
                String path = getSubSchemaPath(record, identifier);
                org.apache.avro.Schema listSchema = subSchemas.get(path);
                List<? super Object> listValue = Objects.requireNonNull(record.getList(identifier, CodecRegistry.DEFAULT.codecFor(listType.getElementType()).getJavaType().getRawType()))
                        .stream().map(this::marshalCollectionValue).collect(Collectors.toList());
                log.debug("field={} listSchema={} listValue={}", identifier, listSchema, listValue);
                return createArrayNode(listSchema, listValue);
            }
            case ProtocolConstants.DataType.SET: {
                SetType setType = (SetType) dataType;
                String path = getSubSchemaPath(record, identifier);
                org.apache.avro.Schema setSchema = subSchemas.get(path);
                Set<Object> setValue = Objects.requireNonNull(record.getSet(identifier, CodecRegistry.DEFAULT.codecFor(setType.getElementType()).getJavaType().getRawType()))
                        .stream().map(this::marshalCollectionValue).collect(Collectors.toSet());
                log.debug("field={} setSchema={} setValue={}", identifier, setSchema, setValue);
                return createArrayNode(setSchema, setValue);
            }
            case ProtocolConstants.DataType.MAP: {
                MapType mapType = (MapType) dataType;
                String path = getSubSchemaPath(record, identifier);
                org.apache.avro.Schema mapSchema = subSchemas.get(path);
                Map<String, JsonNode> map = Objects.requireNonNull(record.getMap(identifier,
                                CodecRegistry.DEFAULT.codecFor(mapType.getKeyType()).getJavaType().getRawType(),
                                CodecRegistry.DEFAULT.codecFor(mapType.getValueType()).getJavaType().getRawType()))
                        .entrySet().stream().collect(
                                Collectors.toMap(
                                        e -> stringify(mapType.getKeyType(), e.getKey()),
                                        e -> toJson(mapSchema.getValueType(), marshalCollectionValue(e.getValue())))
                        );
                log.debug("field={} mapSchema={} mapValue={}", identifier, mapSchema, map);
                ObjectNode objectNode = jsonNodeFactory.objectNode();
                map.forEach(objectNode::set);
                return objectNode;
            }
            default:
                log.debug("Ignoring unsupported column name={} type={}", identifier, dataType.asCql(false, true));
        }

        return null;
    }

    private String getSubSchemaPath(GettableById record, CqlIdentifier identifier) {
        if (record instanceof UdtValue) {
            UdtValue udtValue = (UdtValue) record;
            String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName();
            return typeName + "." + identifier.toString();
        }
        return identifier.toString();
    }

    private static byte[] getBytes(ByteBuffer bf) {
        byte[] bytes = new byte[bf.remaining()];
        bf.get(bytes);
        return bytes;
    }

    private JsonNode buildUDTValue(UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName();
        org.apache.avro.Schema udtSchema = subSchemas.get(typeName);
        Preconditions.checkNotNull(udtSchema, "Schema not found for UDT=" + typeName);
        Preconditions.checkState(udtSchema.getFields().size() > 0, "Schema UDT=" + typeName + " has no fields");
        return buildUDTValue(udtSchema, udtValue);
    }

    JsonNode buildUDTValue(org.apache.avro.Schema udtSchema, UdtValue udtValue) {
        ObjectNode node = jsonNodeFactory.objectNode();
        List<String> fields = udtSchema.getFields().stream().map(org.apache.avro.Schema.Field::name).collect(Collectors.toList());
        for(CqlIdentifier field : udtValue.getType().getFieldNames()) {
            DataType dataType = udtValue.getType(field);
            if (fields.contains(field.asInternal()) && !udtValue.isNull(field)) {
                node.set(field.toString(), toJson(udtValue, field, dataType));
            }
        }

        return node;
    }

    JsonNode createArrayNode(org.apache.avro.Schema schema, Collection collection) {
        ArrayNode node = JsonNodeFactory.instance.arrayNode(collection.size());
        for(Object element : collection) {
            if (element instanceof UdtValue) {
                UdtValue udtValue = (UdtValue) element;
                node.add(buildUDTValue(udtValue));
            } else if (element instanceof Set) {
                node.add(createArrayNode(schema.getElementType(), (Collection)element));
            } else if (element instanceof List) {
                node.add(createArrayNode(schema.getElementType(), (Collection)element));
            } else {
                JsonNode fieldValue = toJson(schema.getElementType(), element);
                node.add(fieldValue);
            }
        }

        return node;
    }

    public static byte[] serializeJsonNode(JsonNode node) {
        try {
            return mapper.writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] fromConnectData(GenericRecord genericRecord) {
        if (genericRecord == null) {
            return null;
        }
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        for (org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
            objectNode.set(field.name(), toJson(field.schema(), genericRecord.get(field.name())));
        }
        return serializeJsonNode(objectNode);
    }

    private static Map<String, LogicalTypeConverter<?>> logicalTypeConverters = new HashMap<>();

    private static JsonNode cqlDurationToJsonNode(CqlDuration cqlDuration) {
        ObjectNode object = jsonNodeFactory.objectNode();
        object.put(CqlLogicalTypes.CQL_DURATION_MONTHS, cqlDuration.getMonths());
        object.put(CqlLogicalTypes.CQL_DURATION_DAYS, cqlDuration.getDays());
        object.put(CqlLogicalTypes.CQL_DURATION_NANOSECONDS, cqlDuration.getNanoseconds());
        return object;
    }

    private static JsonNode bigDecimalToJsonNode(BigDecimal bigDecimal) {
        ObjectNode decimalNode = jsonNodeFactory.objectNode();
        decimalNode.put(CqlLogicalTypes.CQL_DECIMAL_BIGINT, bigDecimal.unscaledValue().toByteArray())
                .put(CqlLogicalTypes.CQL_DECIMAL_SCALE, bigDecimal.scale());

        return decimalNode;
    }
    public static JsonNode toJson(org.apache.avro.Schema schema, Object value) {
        // schema.getLogicalType() always returns null although the name would be populated with the logical type
        // TODO: Use logical type instead of name https://github.com/datastax/cdc-apache-cassandra/issues/85
        if (schema.getName() != null && logicalTypeConverters.containsKey(schema.getName())) {
            return logicalTypeConverters.get(schema.getName()).toJson(value);
        }

        if (value == null) {
            return jsonNodeFactory.nullNode();
        }
        switch(schema.getType()) {
            case NULL: // this should not happen
                return jsonNodeFactory.nullNode();
            case INT:
                return jsonNodeFactory.numberNode((Integer) value);
            case LONG:
                return jsonNodeFactory.numberNode((Long) value);
            case DOUBLE:
                return jsonNodeFactory.numberNode((Double) value);
            case FLOAT:
                return jsonNodeFactory.numberNode((Float) value);
            case BOOLEAN:
                return jsonNodeFactory.booleanNode((Boolean) value);
            case BYTES:
                return jsonNodeFactory.binaryNode(getBytes((ByteBuffer) value));
            case FIXED:
                return jsonNodeFactory.binaryNode(((GenericFixed) value).bytes());
            case ENUM: // GenericEnumSymbol
            case STRING:
                return jsonNodeFactory.textNode(value.toString()); // can be a String or org.apache.avro.util.Utf8
            case ARRAY:
                org.apache.avro.Schema elementSchema = schema.getElementType();
                ArrayNode arrayNode = jsonNodeFactory.arrayNode();
                Object[] iterable;
                if (value instanceof GenericData.Array) {
                    iterable = ((GenericData.Array) value).toArray();
                } else {
                    iterable = (Object[]) value;
                }
                for (Object elem : iterable) {
                    JsonNode fieldValue = toJson(elementSchema, elem);
                    arrayNode.add(fieldValue);
                }
                return arrayNode;
            case MAP:
                @SuppressWarnings("unchecked") Map<Object, Object> map = (Map<Object, Object>) value;
                ObjectNode objectNode = jsonNodeFactory.objectNode();
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    JsonNode jsonNode = toJson(schema.getValueType(), entry.getValue());
                    // can be a String or org.apache.avro.util.Utf8
                    final String entryKey = entry.getKey() == null ? null : entry.getKey().toString();
                    objectNode.set(entryKey, jsonNode);
                }
                return objectNode;
            case RECORD:
                return toJson((GenericRecord) value);
            case UNION:
                for (org.apache.avro.Schema s : schema.getTypes()) {
                    if (s.getType() == org.apache.avro.Schema.Type.NULL) {
                        continue;
                    }
                    return toJson(s, value);
                }
                // this case should not happen
                return jsonNodeFactory.textNode(value.toString());
            default:
                throw new UnsupportedOperationException("Unknown AVRO schema type=" + schema.getType());
        }
    }

    public static JsonNode toJson(GenericRecord genericRecord) {
        if (genericRecord == null) {
            return null;
        }
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        for (org.apache.avro.Schema.Field field : genericRecord.getSchema().getFields()) {
            objectNode.set(field.name(), toJson(field.schema(), genericRecord.get(field.name())));
        }
        return objectNode;
    }


    public boolean isSupportedCqlType(DataType dataType) {
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
            case ProtocolConstants.DataType.BOOLEAN:
            case ProtocolConstants.DataType.BLOB:
            case ProtocolConstants.DataType.DATE:
            case ProtocolConstants.DataType.TIME:
            case ProtocolConstants.DataType.TIMESTAMP:
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
            case ProtocolConstants.DataType.TINYINT:
            case ProtocolConstants.DataType.SMALLINT:
            case ProtocolConstants.DataType.INT:
            case ProtocolConstants.DataType.BIGINT:
            case ProtocolConstants.DataType.DOUBLE:
            case ProtocolConstants.DataType.FLOAT:
            case ProtocolConstants.DataType.INET:
            case ProtocolConstants.DataType.UDT:
            case ProtocolConstants.DataType.VARINT:
            case ProtocolConstants.DataType.DECIMAL:
            case ProtocolConstants.DataType.DURATION:
            case ProtocolConstants.DataType.LIST:
            case ProtocolConstants.DataType.SET:
            case ProtocolConstants.DataType.MAP:
                return true;
        }
        return false;
    }

    abstract static class LogicalTypeConverter<T> {
        final Conversion<T> conversion;

        public LogicalTypeConverter(Conversion<T> conversion) {
            this.conversion = conversion;
        }

        abstract JsonNode toJson(Object value);
    }

    static {
        logicalTypeConverters.put(CqlLogicalTypes.CQL_DECIMAL, new NativeJsonConverter.LogicalTypeConverter<BigDecimal>(
                new Conversions.DecimalConversion()) {

            /**
             * @return a json node representing a struct: {"bigint":"<base64 encoded bytes>", "scale":"<int scale of the big decimal>"}
             */
            @Override
            JsonNode toJson(Object value) {
                Conversion<BigDecimal> conversion = SpecificData.get().getConversionByClass(BigDecimal.class);
                value =  conversion.fromRecord( (IndexedRecord) value, CqlLogicalTypes.decimalType, CqlLogicalTypes.CQL_DECIMAL_LOGICAL_TYPE);

                if (!(value instanceof BigDecimal)) {
                    throw new IllegalArgumentException("Invalid type for Decimal, expected BigDecimal but was "
                            + value.getClass());
                }

                return bigDecimalToJsonNode((BigDecimal) value);
            }
        });
        logicalTypeConverters.put(CqlLogicalTypes.CQL_DURATION, new NativeJsonConverter.LogicalTypeConverter<BigDecimal>(
                new Conversions.DecimalConversion()) {

            /**
             * @return a json node representing a duration: {"month":"<int encoded months>", "days":"<int encoded days>", "nanoseconds":"<long encoded nanos>"}
             */
            @Override
            JsonNode toJson(Object value) {
                Conversion<CqlDuration> conversion = SpecificData.get().getConversionByClass(CqlDuration.class);
                value =  conversion.fromRecord( (IndexedRecord) value, CqlLogicalTypes.durationType, CqlLogicalTypes.CQL_DECIMAL_LOGICAL_TYPE);

                if (!(value instanceof CqlDuration)) {
                    throw new IllegalArgumentException("Invalid type for Duration, expected CqlDuration but was "
                            + value.getClass());
                }

                return cqlDurationToJsonNode((CqlDuration) value);
            }
        });
    }
}
