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

import com.datastax.oss.cdc.backfill.util.TableUtils;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.util.List;

public class ExportedTable {

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

    @Override
    public String toString() {
        return fullyQualifiedName;
    }
}
