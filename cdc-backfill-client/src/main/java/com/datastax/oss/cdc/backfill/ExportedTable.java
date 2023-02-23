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

package com.datastax.oss.cdc.backfill;

import com.amazonaws.services.kms.model.UnsupportedOperationException;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.google.common.collect.ImmutableMap;
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
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;

import java.util.List;

public class ExportedTable {

    /**
     * See {@link com.datastax.oss.cdc.agent.PulsarMutationSender#avroSchemaTypes for full list of supported PK types
     * by the agent
     */
    private static final ImmutableMap<String, AbstractType<?>> abstractTypes = ImmutableMap.<String, AbstractType<?>>builder()
            .put(UTF8Type.instance.asCQL3Type().toString(), UTF8Type.instance)
            .put(AsciiType.instance.asCQL3Type().toString(), AsciiType.instance)
            .put(BooleanType.instance.asCQL3Type().toString(), BooleanType.instance)
            .put(BytesType.instance.asCQL3Type().toString(), BytesType.instance)
            .put(ByteType.instance.asCQL3Type().toString(), ByteType.instance)
            .put(ShortType.instance.asCQL3Type().toString(), ShortType.instance)
            .put(Int32Type.instance.asCQL3Type().toString(), Int32Type.instance)
            .put(IntegerType.instance.asCQL3Type().toString(), IntegerType.instance)
            .put(LongType.instance.asCQL3Type().toString(), LongType.instance)
            .put(FloatType.instance.asCQL3Type().toString(), FloatType.instance)
            .put(DoubleType.instance.asCQL3Type().toString(), DoubleType.instance)
            .put(DecimalType.instance.asCQL3Type().toString(), DecimalType.instance)
            .put( InetAddressType.instance.asCQL3Type().toString(), InetAddressType.instance)
            .put(TimestampType.instance.asCQL3Type().toString(), TimestampType.instance)
            .put(SimpleDateType.instance.asCQL3Type().toString(), SimpleDateType.instance)
            .put(TimeType.instance.asCQL3Type().toString(), TimeType.instance)
            .put(DurationType.instance.asCQL3Type().toString(), DurationType.instance)
            .put(UUIDType.instance.asCQL3Type().toString(), UUIDType.instance)
            .put(TimeUUIDType.instance.asCQL3Type().toString(), TimeUUIDType.instance)
            .build();

    public final KeyspaceMetadata keyspace;
    public final TableMetadata table;
    public final List<ExportedColumn> columns;
    public final String fullyQualifiedName;

    public ExportedTable(
            KeyspaceMetadata keyspace, TableMetadata table, List<ExportedColumn> columns) {
        this.table = table;
        this.keyspace = keyspace;
        this.columns = columns;
        this.fullyQualifiedName =
                String.format("%s.%s", table.getKeyspace().asCql(true), table.getName().asCql(true));
    }

    /**
     * Adapts the {@link TableMetadata} to {@link org.apache.cassandra.schema.TableMetadata} to be used with pulsar
     * importer
     */
    public org.apache.cassandra.schema.TableMetadata getCassandraSchemaTable() {
        org.apache.cassandra.schema.TableMetadata.Builder builder =
                org.apache.cassandra.schema.TableMetadata.builder(
                        keyspace.getName().toString(),
                        table.getName().toString());
        this.table.getPartitionKey().forEach(k ->
                builder.addPartitionKeyColumn(
                        k.getName().toString(),
                        getAbstractDataType(k.getType().asCql(false, true))));
        this.table.getClusteringColumns().forEach((k, __) ->
                builder.addClusteringColumn(k.getName().toString(),
                        getAbstractDataType(k.getType().asCql(false, true))));
        return builder.build();
    }

    private AbstractType<?>  getAbstractDataType(String asCql) {
        if (!abstractTypes.containsKey(asCql)) {
            throw new UnsupportedOperationException("Unsupported column type: " + asCql);
        }

        return abstractTypes.get(asCql);
    }

    @Override
    public String toString() {
        return fullyQualifiedName;
    }
}
