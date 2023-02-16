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
package com.datastax.oss.cdc.backfill.util;

import com.datastax.oss.cdc.backfill.BackFillSettings;
import com.datastax.oss.cdc.backfill.ClusterInfo;
import com.datastax.oss.cdc.backfill.Credentials;
import com.datastax.oss.cdc.backfill.ExportedColumn;
import com.datastax.oss.cdc.backfill.ExportedTable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelUtils {

    public static final Logger LOGGER = LoggerFactory.getLogger(ModelUtils.class);

    public static ExportedTable buildExportedTable(
            ClusterInfo origin, Credentials credentials, BackFillSettings inclusionSettings) {
        try (CqlSession session = SessionUtils.createSession(origin, credentials)) {
            LOGGER.info("Tables to migrate:");
            KeyspaceMetadata keyspace = getExportedKeyspace(session, inclusionSettings);
            TableMetadata table = getExportedTablesInKeyspace(keyspace, inclusionSettings);
            List<ExportedColumn> exportedColumns = buildExportedPKColumns(table);
            ExportedTable exportedTable = new ExportedTable(keyspace, table, exportedColumns);
            LOGGER.info(
                    "- {} ({})", exportedTable, "regular");
            return exportedTable;
        }
    }

    private static KeyspaceMetadata getExportedKeyspace(
            CqlSession session, BackFillSettings settings) {
        return session.getMetadata().getKeyspace(settings.keyspace).get();
    }

    private static TableMetadata getExportedTablesInKeyspace(
            KeyspaceMetadata keyspace, BackFillSettings settings) {

        //TableType tableType = settings.getTableType();
        return keyspace.getTable(settings.table).get();
    }

    private static List<ExportedColumn> buildExportedPKColumns(
            TableMetadata table) {
        List<ExportedColumn> exportedColumns = new ArrayList<>();
        for (ColumnMetadata pk : table.getPrimaryKey()) {
            exportedColumns.add(new ExportedColumn(pk, true, null, null));
        }
        return exportedColumns;
    }
}
