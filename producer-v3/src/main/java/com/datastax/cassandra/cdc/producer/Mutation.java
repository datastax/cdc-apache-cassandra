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
package com.datastax.cassandra.cdc.producer;

import lombok.NoArgsConstructor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@NoArgsConstructor
public class Mutation extends AbstractMutation<CFMetaData> {

    public Mutation(UUID nodeId, Long segment, int position, RowData data, long tsMicro, String md5Digest, CFMetaData t, Object token) {
        super(nodeId, segment, position, data, tsMicro, md5Digest, t, token);
    }

    @Override
    public String name() {
        return metadata.cfName;
    }

    @Override
    public String keyspace() {
        return metadata.ksName;
    }

    @Override
    public List<ColumnInfo> primaryKeyColumns() {
        List<ColumnInfo> columnInfos = new ArrayList<>();
        for(ColumnDefinition cm :metadata.primaryKeyColumns())
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
