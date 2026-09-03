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

import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SchemaChangeListener} with default no-op bodies for every callback neither the Pulsar
 * nor the Kafka source connector acts on, plus shared default implementations for the three
 * callbacks that do affect query/converter state.
 * <p>
 * Implementers must provide {@link #getKeyspaceName()}, {@link #getTableName()},
 * {@link #getCassandraClient()}, and {@link #setValueConverterAndQuery(KeyspaceMetadata, TableMetadata)}.
 */
public interface SourceSchemaChangeListener extends SchemaChangeListener {

    Logger LOG = LoggerFactory.getLogger(SourceSchemaChangeListener.class);

    String getKeyspaceName();
    String getTableName();
    CassandraClient getCassandraClient();
    void setValueConverterAndQuery(KeyspaceMetadata ksm, TableMetadata tableMetadata);

    @Override
    default void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspace) {
    }

    @Override
    default void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
    }

    @Override
    default void onKeyspaceUpdated(@NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
    }

    @Override
    default void onTableCreated(@NonNull TableMetadata table) {
    }

    @Override
    default void onTableDropped(@NonNull TableMetadata table) {
    }

    @Override
    default void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {
        LOG.debug("onUserDefinedTypeDropped {}", type);
    }

    @Override
    default void onFunctionCreated(@NonNull FunctionMetadata function) {
    }

    @Override
    default void onFunctionDropped(@NonNull FunctionMetadata function) {
    }

    @Override
    default void onFunctionUpdated(@NonNull FunctionMetadata current, @NonNull FunctionMetadata previous) {
    }

    @Override
    default void onAggregateCreated(@NonNull AggregateMetadata aggregate) {
    }

    @Override
    default void onAggregateDropped(@NonNull AggregateMetadata aggregate) {
    }

    @Override
    default void onAggregateUpdated(@NonNull AggregateMetadata current, @NonNull AggregateMetadata previous) {
    }

    @Override
    default void onViewCreated(@NonNull ViewMetadata view) {
    }

    @Override
    default void onViewDropped(@NonNull ViewMetadata view) {
    }

    @Override
    default void onViewUpdated(@NonNull ViewMetadata current, @NonNull ViewMetadata previous) {
    }

    @SneakyThrows
    @Override
    default void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
        LOG.debug("onTableUpdated {} {}", current, previous);
        if (current.getKeyspace().asInternal().equals(getKeyspaceName())
                && current.getName().asInternal().equals(getTableName())) {
            KeyspaceMetadata ksm = getCassandraClient().getCqlSession().getMetadata().getKeyspace(current.getKeyspace()).get();
            setValueConverterAndQuery(ksm, current);
        }
    }

    @SneakyThrows
    @Override
    default void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        LOG.debug("onUserDefinedTypeCreated {}", type);
        if (type.getKeyspace().asInternal().equals(getKeyspaceName())) {
            KeyspaceMetadata ksm = getCassandraClient().getCqlSession().getMetadata().getKeyspace(type.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(getTableName()).get());
        }
    }

    @SneakyThrows
    @Override
    default void onUserDefinedTypeUpdated(@NonNull UserDefinedType userDefinedType, @NonNull UserDefinedType userDefinedType1) {
        LOG.debug("onUserDefinedTypeUpdated {} {}", userDefinedType, userDefinedType1);
        if (userDefinedType.getKeyspace().asCql(true).equals(getKeyspaceName())) {
            KeyspaceMetadata ksm = getCassandraClient().getCqlSession().getMetadata().getKeyspace(userDefinedType.getKeyspace()).get();
            setValueConverterAndQuery(ksm, ksm.getTable(getTableName()).get());
        }
    }
}
