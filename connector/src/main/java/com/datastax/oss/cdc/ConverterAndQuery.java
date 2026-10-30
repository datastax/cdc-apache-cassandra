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

import com.datastax.oss.cdc.converters.ConverterFactory;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Bundles a table's row converter with the CQL projection/primary-key clauses and prepared
 * statements needed to read back a mutated row, shared by the Pulsar and Kafka source
 * connectors. {@code C} is each platform's own converter type (they don't share a common
 * interface, so this is left an unconstrained type parameter).
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class ConverterAndQuery<C> {
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
    final C converter;

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

    final ConcurrentMap<Integer, PreparedStatement> preparedStatements;

    /**
     * When requesting a partition, the projection clause contains only static columns.
     * When requesting a wide row, the projection clause contains regular and static columns
     * When deleting a single row or a partition, the projection contains regular and static columns
     * @param whereClauseLength number of columns in the CQL where clause.
     * @return the projection clause
     */
    public CqlIdentifier[] getProjectionClause(int whereClauseLength) {
        // when primary key columns are different from where clause columns and static columns are absent, we still
        // need to include regular columns in the projection clause (e.g. for DELETE by partition key use cases)
        return primaryKeyClause.length == whereClauseLength || staticProjectionClause.length == 0
                ? projectionClause
                : staticProjectionClause;
    }

    /**
     * Builds (or reuses a cached) prepared SELECT statement for the given where-clause length.
     */
    public PreparedStatement prepareSelectStatement(CassandraClient cassandraClient, int whereClauseLength) {
        return preparedStatements.computeIfAbsent(whereClauseLength, k ->
                cassandraClient.prepareSelect(keyspaceName, tableName, getProjectionClause(whereClauseLength), primaryKeyClause, k));
    }

    /**
     * Computes the replicated columns for a table (honoring {@code columns.regexp} and the
     * json-only/primary-key-only output rules) and builds the {@link ConverterAndQuery} for it.
     * Shared by both platforms' schema-change handling, since only the converter class differs.
     */
    public static <C> ConverterAndQuery<C> forTable(
            CassandraSourceConnectorConfig config,
            Optional<Pattern> columnPattern,
            CassandraClient cassandraClient,
            KeyspaceMetadata ksm,
            TableMetadata tableMetadata,
            Class<?> converterClass,
            Logger log)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        boolean isPrimaryKeyOnlyTable = CassandraClient.isPrimaryKeyOnlyTable(tableMetadata);
        List<ColumnMetadata> columns = tableMetadata.getColumns().values().stream()
                // include primary keys in the json only output format options
                // TODO: PERF: Infuse the key values instead of reading from DB https://github.com/datastax/cdc-apache-cassandra/issues/84
                // If primary key only table, then add all the columns into the value schema.
                .filter(c -> config.isJsonOnlyOutputFormat() || isPrimaryKeyOnlyTable || !tableMetadata.getPrimaryKey().contains(c))
                .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                .collect(Collectors.toList());
        List<ColumnMetadata> staticColumns = tableMetadata.getColumns().values().stream()
                .filter(ColumnMetadata::isStatic)
                .filter(c -> !tableMetadata.getPrimaryKey().contains(c))
                .filter(c -> !columnPattern.isPresent() || columnPattern.get().matcher(c.getName().asInternal()).matches())
                .collect(Collectors.toList());
        log.info("Schema update for table {}.{} replicated columns={}", ksm.getName(), tableMetadata.getName(),
                columns.stream().map(c -> c.getName().asInternal()).collect(Collectors.toList()));
        C converter = ConverterFactory.create(converterClass, ksm, tableMetadata, columns);
        return new ConverterAndQuery<>(
                tableMetadata.getKeyspace().asInternal(),
                tableMetadata.getName().asInternal(),
                converter,
                cassandraClient.buildProjectionClause(columns),
                cassandraClient.buildProjectionClause(staticColumns),
                cassandraClient.buildPrimaryKeyClause(tableMetadata),
                new ConcurrentHashMap<>());
    }
}
