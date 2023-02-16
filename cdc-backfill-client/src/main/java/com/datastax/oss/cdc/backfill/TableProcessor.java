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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.Iterator;

public abstract class TableProcessor {

    protected final ExportedTable exportedTable;

    public TableProcessor(ExportedTable exportedTable) {
        this.exportedTable = exportedTable;
    }

    public ExportedTable getExportedTable() {
        return exportedTable;
    }

    protected String buildExportQuery() {
        StringBuilder builder = new StringBuilder("\"SELECT ");
        Iterator<ExportedColumn> cols = exportedTable.columns.iterator();
        while (cols.hasNext()) {
            ExportedColumn exportedColumn = cols.next();
            String name = escape(exportedColumn.col.getName());
            builder.append(name);
            if (exportedColumn.writetime != null) {
                builder.append(", WRITETIME(");
                builder.append(name);
                builder.append(") AS ");
                builder.append(escape(exportedColumn.writetime));
            }
            if (exportedColumn.ttl != null) {
                builder.append(", TTL(");
                builder.append(name);
                builder.append(") AS ");
                builder.append(escape(exportedColumn.ttl));
            }
            if (cols.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(" FROM ");
        builder.append(escape(exportedTable.keyspace.getName()));
        builder.append(".");
        builder.append(escape(exportedTable.table.getName()));
        builder.append("\"");
        return builder.toString();
    }

    protected String escape(CqlIdentifier id) {
        return escape(id.asCql(true));
    }

    protected abstract String escape(String id);
}
