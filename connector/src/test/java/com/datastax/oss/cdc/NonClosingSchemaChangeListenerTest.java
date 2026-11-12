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
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The driver closes its schema change listener whenever a session closes, including one that
 * failed to initialize; that must not reach the source, which registers itself as the listener.
 */
class NonClosingSchemaChangeListenerTest {

    @Test
    void doesNotForwardClose() throws Exception {
        SchemaChangeListener source = mock(SchemaChangeListener.class);

        new NonClosingSchemaChangeListener(source).close();

        verify(source, never()).close();
    }

    @Test
    void forwardsSchemaEvents() {
        SchemaChangeListener source = mock(SchemaChangeListener.class);
        NonClosingSchemaChangeListener listener = new NonClosingSchemaChangeListener(source);
        KeyspaceMetadata ks = mock(KeyspaceMetadata.class);
        TableMetadata table = mock(TableMetadata.class);
        TableMetadata previousTable = mock(TableMetadata.class);
        UserDefinedType udt = mock(UserDefinedType.class);
        FunctionMetadata function = mock(FunctionMetadata.class);
        AggregateMetadata aggregate = mock(AggregateMetadata.class);
        ViewMetadata view = mock(ViewMetadata.class);
        Session session = mock(Session.class);

        listener.onKeyspaceCreated(ks);
        listener.onKeyspaceDropped(ks);
        listener.onKeyspaceUpdated(ks, ks);
        listener.onTableCreated(table);
        listener.onTableDropped(table);
        listener.onTableUpdated(table, previousTable);
        listener.onUserDefinedTypeCreated(udt);
        listener.onUserDefinedTypeDropped(udt);
        listener.onUserDefinedTypeUpdated(udt, udt);
        listener.onFunctionCreated(function);
        listener.onFunctionDropped(function);
        listener.onFunctionUpdated(function, function);
        listener.onAggregateCreated(aggregate);
        listener.onAggregateDropped(aggregate);
        listener.onAggregateUpdated(aggregate, aggregate);
        listener.onViewCreated(view);
        listener.onViewDropped(view);
        listener.onViewUpdated(view, view);
        listener.onSessionReady(session);

        verify(source).onKeyspaceCreated(ks);
        verify(source).onKeyspaceDropped(ks);
        verify(source).onKeyspaceUpdated(ks, ks);
        verify(source).onTableCreated(table);
        verify(source).onTableDropped(table);
        verify(source).onTableUpdated(table, previousTable);
        verify(source).onUserDefinedTypeCreated(udt);
        verify(source).onUserDefinedTypeDropped(udt);
        verify(source).onUserDefinedTypeUpdated(udt, udt);
        verify(source).onFunctionCreated(function);
        verify(source).onFunctionDropped(function);
        verify(source).onFunctionUpdated(function, function);
        verify(source).onAggregateCreated(aggregate);
        verify(source).onAggregateDropped(aggregate);
        verify(source).onAggregateUpdated(aggregate, aggregate);
        verify(source).onViewCreated(view);
        verify(source).onViewDropped(view);
        verify(source).onViewUpdated(view, view);
        verify(source).onSessionReady(session);
    }
}
