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
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Forwards schema events to a source but not {@link #close()}.
 *
 * <p>The driver closes its schema change listener whenever a session closes, including a
 * session that failed to initialize. The sources register themselves as the listener, and their
 * {@code close()} shuts the whole source down (the Kafka task closes its events consumer), so a
 * single failed connection attempt during a startup retry would leave the source unusable.
 * The source's lifecycle is managed by its runtime instead.
 */
public class NonClosingSchemaChangeListener implements SchemaChangeListener {

    private final SchemaChangeListener delegate;

    public NonClosingSchemaChangeListener(SchemaChangeListener delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspace) {
        delegate.onKeyspaceCreated(keyspace);
    }

    @Override
    public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
        delegate.onKeyspaceDropped(keyspace);
    }

    @Override
    public void onKeyspaceUpdated(@NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
        delegate.onKeyspaceUpdated(current, previous);
    }

    @Override
    public void onTableCreated(@NonNull TableMetadata table) {
        delegate.onTableCreated(table);
    }

    @Override
    public void onTableDropped(@NonNull TableMetadata table) {
        delegate.onTableDropped(table);
    }

    @Override
    public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
        delegate.onTableUpdated(current, previous);
    }

    @Override
    public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
        delegate.onUserDefinedTypeCreated(type);
    }

    @Override
    public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {
        delegate.onUserDefinedTypeDropped(type);
    }

    @Override
    public void onUserDefinedTypeUpdated(@NonNull UserDefinedType current, @NonNull UserDefinedType previous) {
        delegate.onUserDefinedTypeUpdated(current, previous);
    }

    @Override
    public void onFunctionCreated(@NonNull FunctionMetadata function) {
        delegate.onFunctionCreated(function);
    }

    @Override
    public void onFunctionDropped(@NonNull FunctionMetadata function) {
        delegate.onFunctionDropped(function);
    }

    @Override
    public void onFunctionUpdated(@NonNull FunctionMetadata current, @NonNull FunctionMetadata previous) {
        delegate.onFunctionUpdated(current, previous);
    }

    @Override
    public void onAggregateCreated(@NonNull AggregateMetadata aggregate) {
        delegate.onAggregateCreated(aggregate);
    }

    @Override
    public void onAggregateDropped(@NonNull AggregateMetadata aggregate) {
        delegate.onAggregateDropped(aggregate);
    }

    @Override
    public void onAggregateUpdated(@NonNull AggregateMetadata current, @NonNull AggregateMetadata previous) {
        delegate.onAggregateUpdated(current, previous);
    }

    @Override
    public void onViewCreated(@NonNull ViewMetadata view) {
        delegate.onViewCreated(view);
    }

    @Override
    public void onViewDropped(@NonNull ViewMetadata view) {
        delegate.onViewDropped(view);
    }

    @Override
    public void onViewUpdated(@NonNull ViewMetadata current, @NonNull ViewMetadata previous) {
        delegate.onViewUpdated(current, previous);
    }

    @Override
    public void onSessionReady(@NonNull Session session) {
        delegate.onSessionReady(session);
    }

    @Override
    public void close() {
        // Intentionally not forwarded, see the class comment.
    }
}
