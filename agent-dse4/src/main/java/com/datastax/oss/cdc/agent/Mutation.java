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

import lombok.NoArgsConstructor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@NoArgsConstructor
public class Mutation extends AbstractMutation<TableMetadata> {

    public Mutation(UUID nodeId, Long segment, int position, Object[] pkValues, long tsMicro, String md5Digest, TableMetadata t, Object token) {
        super(nodeId, segment, position, pkValues, tsMicro, md5Digest, t, token);
    }

    @Override
    public String key() {
        return metadata.keyspace + "." + metadata.name;
    }

    @Override
    public String name() {
        return metadata.name;
    }

    @Override
    public String keyspace() {
        return metadata.keyspace;
    }

    @Override
    public List<ColumnInfo> primaryKeyColumns() {
        List<ColumnInfo> columnInfos = new ArrayList<>();
        for(ColumnMetadata cm : metadata.primaryKeyColumns())
            columnInfos.add(new ColumnInfo() {
                @Override
                public String name() {
                    return cm.name.toString();
                }

                @Override
                public String cql3Type() {
                    return cm.type.asCQL3Type().toString();
                }

                @Override
                public boolean isClusteringKey() {
                    return cm.isClusteringColumn();
                }
            });
        return columnInfos;
    }
}
