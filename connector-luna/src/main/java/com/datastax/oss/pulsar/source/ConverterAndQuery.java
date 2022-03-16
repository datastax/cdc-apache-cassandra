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
package com.datastax.oss.pulsar.source;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;

import java.util.concurrent.ConcurrentMap;

@Data
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class ConverterAndQuery {
    /**
     * Keyspace name
     */
    final String keyspaceName;

    /**
     * Table name
     */
    final String tableName;

    /**
     * Schema converter
     */
    final Converter converter;

    /**
     * Projection clause with regular and static columns.
     */
    final CqlIdentifier[] projectionClause;

    /**
     * Projection clause with only static columns.
     */
    final CqlIdentifier[] staticProjectionClause;

    /**
     * Primary key columns
     */
    final CqlIdentifier[] primaryKeyClause;


    /**
     * Cache of prepared statements where key is the number of primary keys.
     */
    final ConcurrentMap<Integer, PreparedStatement> preparedStatements;

    /**
     * KeyValue schema.
     */
    final Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema;

    /**
     * When requesting a partition, the projection clause contains only static columns.
     * When requesting a wide row, the projection clause contains regular and static columns
     * @param whereClauseLength number of columns in the CQL where clause.
     * @return the projection clause
     */
    public CqlIdentifier[] getProjectionClause(int whereClauseLength) {
        return primaryKeyClause.length == whereClauseLength
                ? projectionClause
                : staticProjectionClause;
    }
}
