package com.datastax.oss.pulsar.source.converters;

import com.datastax.oss.cdc.CqlLogicalTypes;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            node.set(fieldName, toJson(row, cm));
        }
        return serializeJsonNode(node);
    }

    JsonNode toJson(Row row, ColumnDefinition cm) {
        if (row.isNull(cm.getName())) {
            return jsonNodeFactory.nullNode();
        }
        switch (cm.getType().getProtocolCode()) {
            case ProtocolConstants.DataType.UUID:
            case ProtocolConstants.DataType.TIMEUUID:
                return jsonNodeFactory.textNode(row.getUuid(cm.getName()).toString());
            case ProtocolConstants.DataType.ASCII:
            case ProtocolConstants.DataType.VARCHAR:
                return jsonNodeFactory.textNode(row.getString(cm.getName()));
            case ProtocolConstants.DataType.TINYINT:
                return jsonNodeFactory.numberNode((int) row.getByte(cm.getName()));
            case ProtocolConstants.DataType.SMALLINT:
                return jsonNodeFactory.numberNode((int) row.getShort(cm.getName()));
            case ProtocolConstants.DataType.INT:
                return jsonNodeFactory.numberNode(row.getInt(cm.getName()));
            case ProtocolConstants.DataType.BIGINT:
                return jsonNodeFactory.numberNode(row.getLong(cm.getName()));
            case ProtocolConstants.DataType.INET:
                return jsonNodeFactory.textNode(row.getInetAddress(cm.getName()).getHostAddress());
            case ProtocolConstants.DataType.DOUBLE:
                return jsonNodeFactory.numberNode(row.getDouble(cm.getName()));
            case ProtocolConstants.DataType.FLOAT:
                return jsonNodeFactory.numberNode(row.getFloat(cm.getName()));
            case ProtocolConstants.DataType.BOOLEAN:
                return jsonNodeFactory.booleanNode(row.getBoolean(cm.getName()));
            case ProtocolConstants.DataType.TIMESTAMP:
                return jsonNodeFactory.numberNode(row.getInstant(cm.getName()).toEpochMilli());
            case ProtocolConstants.DataType.DATE: // Mimic Avro date (epoch days)
                return jsonNodeFactory.numberNode((int) row.getLocalDate(cm.getName()).toEpochDay());
            case ProtocolConstants.DataType.TIME: // Mimic Avro time (microseconds)
                return jsonNodeFactory.numberNode (row.getLocalTime(cm.getName()).toNanoOfDay() / 1000);
            case ProtocolConstants.DataType.BLOB:
                return jsonNodeFactory.binaryNode(row.getByteBuffer(cm.getName()).array());
            case ProtocolConstants.DataType.UDT:
                return buildUDTValue(row.getUdtValue(cm.getName()));
            case ProtocolConstants.DataType.DURATION:
                return cqlDurationToJsonNode(row.getCqlDuration(cm.getName()));
            case ProtocolConstants.DataType.DECIMAL:
                return bigDecimalToJsonNode(row.getBigDecimal(cm.getName()));
            case ProtocolConstants.DataType.VARINT:
                Conversion<BigInteger> conversion = SpecificData.get().getConversionByClass(BigInteger.class);
                ByteBuffer bytes = conversion.toBytes(row.getBigInteger(cm.getName()), CqlLogicalTypes.varintType, CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE);
                return jsonNodeFactory.binaryNode(bytes.array());
            case ProtocolConstants.DataType.LIST: {
                ListType listType = (ListType) cm.getType();
                String fieldName = cm.getName().toString();
                org.apache.avro.Schema listSchema = subSchemas.get(fieldName);
                List listValue = row.getList(fieldName, CodecRegistry.DEFAULT.codecFor(listType.getElementType()).getJavaType().getRawType());
                log.info("field={} listSchema={} listValue={}", fieldName, listSchema, listValue);
                return createArrayNode(listSchema, listValue);
            }
            case ProtocolConstants.DataType.SET: {
                SetType setType = (SetType) cm.getType();
                String fieldName = cm.getName().toString();
                org.apache.avro.Schema setSchema = subSchemas.get(fieldName);
                Set setValue = row.getSet(fieldName, CodecRegistry.DEFAULT.codecFor(setType.getElementType()).getJavaType().getRawType());
                log.debug("field={} setSchema={} setValue={}", fieldName, setSchema, setValue);
                return createArrayNode(setSchema, setValue);
            }
            case ProtocolConstants.DataType.MAP: {
                MapType mapType = (MapType) cm.getType();
                String fieldName = cm.getName().toString();
                org.apache.avro.Schema mapSchema = subSchemas.get(fieldName);
                Map<String, JsonNode> map = row.getMap(fieldName,
                                CodecRegistry.DEFAULT.codecFor(mapType.getKeyType()).getJavaType().getRawType(),
                                CodecRegistry.DEFAULT.codecFor(mapType.getValueType()).getJavaType().getRawType())
                        .entrySet().stream().collect(Collectors.toMap(e -> stringify(mapType.getKeyType(), e.getKey()), e -> toJson(mapSchema.getValueType(), e.getValue())));
                log.debug("field={} mapSchema={} mapValue={}", fieldName, mapSchema, map);
                ObjectNode objectNode = jsonNodeFactory.objectNode();
                map.forEach((k,v)->objectNode.set(k, v));
                return objectNode;
            }
            default:
                log.debug("Ignoring unsupported column name={} type={}", cm.getName(), cm.getType().asCql(false, true));
        }

        return null;
    }

    private JsonNode buildUDTValue(UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName();
        org.apache.avro.Schema udtSchema = subSchemas.get(typeName);
        Preconditions.checkNotNull(udtSchema, "Schema not found for UDT=" + typeName);
        Preconditions.checkState(udtSchema.getFields().size() > 0, "Schema UDT=" + typeName + " has no fields");
        return buildUDTValue(udtSchema, udtValue);
    }

    JsonNode buildUDTValue(org.apache.avro.Schema udtSchema, UdtValue udtValue) {
        String typeName = udtValue.getType().getKeyspace() + "." + udtValue.getType().getName();
        List<String> fields = udtSchema.getFields().stream().map(org.apache.avro.Schema.Field::name).collect(Collectors.toList());
        ObjectNode node = jsonNodeFactory.objectNode();
        for(CqlIdentifier field : udtValue.getType().getFieldNames()) {
            if (fields.contains(field.asInternal()) && !udtValue.isNull(field)) {
                DataType dataType = udtValue.getType(field);
                switch (dataType.getProtocolCode()) {
                    case ProtocolConstants.DataType.UUID:
                    case ProtocolConstants.DataType.TIMEUUID:
                        node.put(field.toString(), udtValue.getUuid(field).toString());
                        break;
                    case ProtocolConstants.DataType.ASCII:
                    case ProtocolConstants.DataType.VARCHAR:
                        node.put(field.toString(), udtValue.getString(field));
                        break;
                    case ProtocolConstants.DataType.TINYINT:
                        node.put(field.toString(), (int) udtValue.getByte(field));
                        break;
                    case ProtocolConstants.DataType.SMALLINT:
                        node.put(field.toString(), (int) udtValue.getShort(field));
                        break;
                    case ProtocolConstants.DataType.INT:
                        node.put(field.toString(), udtValue.getInt(field));
                        break;
                    case ProtocolConstants.DataType.INET:
                        node.put(field.toString(), udtValue.getInetAddress(field).getHostAddress());
                        break;
                    case ProtocolConstants.DataType.BIGINT:
                        node.put(field.toString(), udtValue.getLong(field));
                        break;
                    case ProtocolConstants.DataType.DOUBLE:
                        node.put(field.toString(), udtValue.getDouble(field));
                        break;
                    case ProtocolConstants.DataType.FLOAT:
                        node.put(field.toString(), udtValue.getFloat(field));
                        break;
                    case ProtocolConstants.DataType.BOOLEAN:
                        node.put(field.toString(), udtValue.getBoolean(field));
                        break;
                    case ProtocolConstants.DataType.TIMESTAMP:
                        node.put(field.toString(), udtValue.getInstant(field).toEpochMilli());
                        break;
                    case ProtocolConstants.DataType.DATE:
                        node.put(field.toString(), (int) udtValue.getLocalDate(field).toEpochDay());
                        break;
                    case ProtocolConstants.DataType.TIME:
                        node.put(field.toString(), (udtValue.getLocalTime(field).toNanoOfDay() / 1000));
                        break;
                    case ProtocolConstants.DataType.BLOB:
                        node.put(field.toString(), udtValue.getByteBuffer(field).array());
                        break;
                    case ProtocolConstants.DataType.UDT:
                        node.set(field.toString(), buildUDTValue(udtValue.getUdtValue(field)));
                        break;
                    case ProtocolConstants.DataType.DURATION:
                        node.set(field.toString(), cqlDurationToJsonNode(udtValue.getCqlDuration(field)));
                        break;
                    case ProtocolConstants.DataType.DECIMAL:
                        node.put(field.toString(), udtValue.getBigDecimal(field));
                        break;
                    case ProtocolConstants.DataType.VARINT:
                        node.put(field.toString(), udtValue.getBigInteger(field));
                        break;
                    case ProtocolConstants.DataType.LIST: {
                        ListType listType = (ListType) udtValue.getType(field);
                        List listValue = udtValue.getList(field, CodecRegistry.DEFAULT.codecFor(listType.getElementType()).getJavaType().getRawType());
                        String path = typeName + "." + field.toString();
                        org.apache.avro.Schema elementSchema = subSchemas.get(path);
                        log.debug("path={} elementSchema={} listType={} listValue={}", path, elementSchema, listType, listValue);
                        node.set(field.toString(), createArrayNode(elementSchema, listValue));
                    }
                    break;
                    case ProtocolConstants.DataType.SET: {
                        SetType setType = (SetType) udtValue.getType(field);
                        Set setValue = udtValue.getSet(field, CodecRegistry.DEFAULT.codecFor(setType.getElementType()).getJavaType().getRawType());
                        String path = typeName + "." + field.toString();
                        org.apache.avro.Schema elementSchema = subSchemas.get(path);
                        log.debug("path={} elementSchema={} setType={} setValue={}", path, elementSchema, setType, setValue);
                        node.set(field.toString(), createArrayNode(elementSchema, setValue));
                    }
                    break;
                    case ProtocolConstants.DataType.MAP: {
                        MapType mapType = (MapType) udtValue.getType(field);
                        Map<String, Object> mapValue = udtValue.getMap(field,
                                        CodecRegistry.DEFAULT.codecFor(mapType.getKeyType()).getJavaType().getRawType(),
                                        CodecRegistry.DEFAULT.codecFor(mapType.getValueType()).getJavaType().getRawType())
                                .entrySet().stream().collect(Collectors.toMap(e -> stringify(mapType.getKeyType(), e.getKey()), Map.Entry::getValue));
                        String path = typeName + "." + field.toString();
                        org.apache.avro.Schema valueSchema = subSchemas.get(path);
                        log.debug("path={} valueSchema={} mapType={} mapValue={}", path, valueSchema, mapType, mapValue);
                        node.set(field.toString(), mapper.valueToTree(mapValue));
                    }
                    break;
                    default:
                        log.debug("Ignoring unsupported type field name={} type={}", field, dataType.asCql(false, true));
                }
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
        // TODO: Use logical type instead of name
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
                return jsonNodeFactory.binaryNode(((ByteBuffer) value).array());
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
