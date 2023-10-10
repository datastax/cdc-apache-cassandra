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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.CqlLogicalTypes;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class PulsarMutationSender extends AbstractPulsarMutationSender<TableMetadata> {

    private static final Map<String, Schema> avroSchemaTypes = new HashMap<>();
    static {
        avroSchemaTypes.put(UTF8Type.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        avroSchemaTypes.put(AsciiType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        avroSchemaTypes.put(BooleanType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN));
        avroSchemaTypes.put(BytesType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES));
        avroSchemaTypes.put(ByteType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));  // INT8 not supported by AVRO
        avroSchemaTypes.put(ShortType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));  // INT16 not supported by AVRO
        avroSchemaTypes.put(Int32Type.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
        avroSchemaTypes.put(IntegerType.instance.asCQL3Type().toString(), CqlLogicalTypes.varintType);
        avroSchemaTypes.put(LongType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
        avroSchemaTypes.put(FloatType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT));
        avroSchemaTypes.put(DoubleType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE));
        avroSchemaTypes.put(DecimalType.instance.asCQL3Type().toString(), CqlLogicalTypes.decimalType);
        avroSchemaTypes.put(InetAddressType.instance.asCQL3Type().toString(), org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        avroSchemaTypes.put(TimestampType.instance.asCQL3Type().toString(), CqlLogicalTypes.timestampMillisType);
        avroSchemaTypes.put(SimpleDateType.instance.asCQL3Type().toString(), CqlLogicalTypes.dateType);
        avroSchemaTypes.put(TimeType.instance.asCQL3Type().toString(), CqlLogicalTypes.timeMicrosType);
        avroSchemaTypes.put(DurationType.instance.asCQL3Type().toString(), CqlLogicalTypes.durationType);
        avroSchemaTypes.put(UUIDType.instance.asCQL3Type().toString(), CqlLogicalTypes.uuidType);
        avroSchemaTypes.put(TimeUUIDType.instance.asCQL3Type().toString(), CqlLogicalTypes.uuidType);
    }



    public PulsarMutationSender(AgentConfig config) {
        super(config, DatabaseDescriptor.getPartitionerName().equals(Murmur3Partitioner.class.getName()));
    }

    @Override
    public void incSkippedMutations() {
        //CdcMetrics.skippedMutations.inc();
    }

    @Override
    public UUID getHostId() {
        return StorageService.instance.getLocalHostUUID();
    }

    @Override
    public org.apache.avro.Schema getNativeSchema(String cql3Type) {
        return avroSchemaTypes.get(cql3Type);
    }

    @Override
    public SchemaAndWriter getPkSchema(String key) {
        return pkSchemas.get(key);
    }

    /**
     * Check the primary key has supported columns.
     * @param mutation
     * @return false if the primary key has unsupported CQL columns
     */
    @Override
    public boolean isSupported(final AbstractMutation<TableMetadata> mutation) {
        if (!pkSchemas.containsKey(mutation.key())) {
            for (ColumnMetadata cm : mutation.metadata.primaryKeyColumns()) {
                if (!avroSchemaTypes.containsKey(cm.type.asCQL3Type().toString())) {
                    log.warn("Unsupported primary key column={}.{}.{} type={}, skipping mutation", cm.ksName, cm.cfName, cm.name, cm.type.asCQL3Type().toString());
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Object cqlToAvro(TableMetadata tableMetadata, String columnName, Object value) {
        ColumnMetadata columnMetadata = tableMetadata.getColumn(ColumnIdentifier.getInterned(columnName, false));
        AbstractType<?> type = columnMetadata.type.isReversed() ? ((ReversedType) columnMetadata.type).baseType : columnMetadata.type;
        log.trace("column name={} type={} class={} value={}",
                columnMetadata.name, type.getClass().getName(),
                value != null ? value.getClass().getName() : null, value);

        if (value == null)
            return null;

        if (type instanceof TimestampType) {
            if (value instanceof Date)
                return ((Date) value).getTime();
            if (value instanceof Instant)
                return ((Instant) value).toEpochMilli();
        }
        if (type instanceof SimpleDateType && value instanceof Integer) {
            long timeInMillis = Duration.ofDays((Integer) value + Integer.MIN_VALUE).toMillis();
            Instant instant = Instant.ofEpochMilli(timeInMillis);
            LocalDate localDate = LocalDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
            return (int) localDate.toEpochDay(); // Avro date is an int that stores the number of days from the unix epoch
        }
        if (type instanceof TimeType && value instanceof Long) {
            return ((Long) value / 1000); // Avro time is in microseconds
        }
        if (type instanceof InetAddressType) {
            return ((InetAddress) value).getHostAddress();
        }
        if (type instanceof ByteType) {
            return Byte.toUnsignedInt((byte) value); // AVRO does not support INT8
        }
        if (type instanceof ShortType) {
            return Short.toUnsignedInt((short) value); // AVRO does not support INT16
        }
        return value;
    }
}
