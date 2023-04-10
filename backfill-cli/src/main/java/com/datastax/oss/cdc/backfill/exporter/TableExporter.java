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
package com.datastax.oss.cdc.backfill.exporter;

import com.datastax.oss.cdc.backfill.BackfillSettings;
import com.datastax.oss.cdc.backfill.ExitStatus;
import com.datastax.oss.cdc.backfill.factory.DsBulkFactory;
import com.datastax.oss.cdc.backfill.factory.SessionFactory;
import com.datastax.oss.cdc.backfill.util.LoggingUtils;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TableExporter {
    private static final URL DSBULK_CONFIGURATION_FILE =
            TableExporter.class.getClassLoader().getResource("logback-dsbulk-embedded.xml");
    private static final Logger LOGGER = LoggerFactory.getLogger(TableExporter.class);

    protected final BackfillSettings settings;

    protected final Path tableDataDir;

    protected final Path exportAckDir;
    protected final Path exportAckFile;

    private final DsBulkFactory dsBulkFactory;

    private final ExportedTable exportedTable;
    private final SessionFactory sessionFactory;

    public  TableExporter(DsBulkFactory dsBulkFactory, SessionFactory sessionFactory, BackfillSettings settings) {
        this.dsBulkFactory = dsBulkFactory;
        this.sessionFactory = sessionFactory;
        this.settings = settings;
        this.exportedTable = buildExportedTable();
        this.tableDataDir =
                settings
                        .dataDir
                        .resolve(exportedTable.getKeyspace().getName().asInternal())
                        .resolve(exportedTable.getTable().getName().asInternal());
        this.exportAckDir = settings.dataDir.resolve("__exported__");
        this.exportAckFile =
                exportAckDir.resolve(
                        exportedTable.getKeyspace().getName().asInternal()
                                + "__"
                                + exportedTable.getTable().getName().asInternal()
                                + ".exported");
    }

    public ExitStatus exportTable() {
        String operationId = retrieveExportOperationId();
        if (operationId != null) {
            LOGGER.warn(
                    "Table {}.{}: already exported, skipping (to re-export, delete the following artifacts: '{}' and '{}').",
                    exportedTable.getKeyspace().getName(),
                    exportedTable.getTable().getName(),
                    exportAckFile,
                    tableDataDir);
            return ExitStatus.STATUS_OK;
        } else {
            LOGGER.info("Exporting {}...", exportedTable);
            operationId = createOperationId();
            List<String> args = createExportArgs(operationId);
            ExitStatus status = invokeDsbulk(operationId, args);
            LOGGER.info("Export of {} finished with {}", exportedTable, status);
            if (status == ExitStatus.STATUS_OK) {
                createExportAckFile(operationId);
            }
            return status;
        }
    }
    
    public boolean isExported() {
        return Files.exists(exportAckFile);
    }

    public ExportedTable getExportedTable() {
        return this.exportedTable;
    }

    public Path getTableDataDir() {
        return tableDataDir;
    }

    private ExitStatus invokeDsbulk(String operationId, List<String> args) {
        int exitCode;
        final DataStaxBulkLoader loader = this.dsBulkFactory.newLoader(args.toArray(new String[0]));
        LoggingUtils.configureLogging(DSBULK_CONFIGURATION_FILE);
        System.setProperty("OPERATION_ID", operationId);
        try {
            exitCode = loader.run().exitCode();
        } finally {
            System.clearProperty("OPERATION_ID");
            LoggingUtils.configureLogging(LoggingUtils.MIGRATOR_CONFIGURATION_FILE);
        }
        return ExitStatus.forCode(exitCode);
    }
    private String createOperationId() {
        ZonedDateTime now = Instant.now().atZone(ZoneOffset.UTC);
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").format(now);
        return String.format(
                "%s_%s_%s_%s",
                "EXPORT" ,
                exportedTable.getKeyspace().getName().asInternal(),
                exportedTable.getTable().getName().asInternal(),
                timestamp);
    }

    private String retrieveExportOperationId() {
        if (isExported()) {
            try {
                String operationId = new String(Files.readAllBytes(exportAckFile));
                if (!(isBlankString(operationId))) {
                    return operationId;
                }
            } catch (IOException ignored) {
            }
        }
        return null;
    }

    boolean isBlankString(String string) {
        return string == null || string.trim().isEmpty();
    }

    private void createExportAckFile(String operationId) {
        try {
            Files.createDirectories(exportAckDir);
            Files.createFile(exportAckFile);
            Files.write(exportAckFile, operationId.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> createExportArgs(String operationId) {
        List<String> args = new ArrayList<>();
        args.add("unload");
        if (settings.exportSettings.clusterInfo.bundle != null) {
            args.add("-b");
            args.add(String.valueOf(settings.exportSettings.clusterInfo.bundle));
        } else {
            args.add("-h");
            String hosts =
                    settings.exportSettings.clusterInfo.hostsAndPorts.stream()
                            .map(hp -> "\"" + hp + "\"")
                            .collect(Collectors.joining(","));
            args.add("[" + hosts + "]");
        }
        if (settings.exportSettings.clusterInfo.protocolVersion != null) {
            args.add("--driver.advanced.protocol.version");
            args.add(settings.exportSettings.clusterInfo.protocolVersion);
        }
        if (settings.exportSettings.credentials != null) {
            args.add("-u");
            args.add(settings.exportSettings.credentials.username);
            args.add("-p");
            args.add(String.valueOf(settings.exportSettings.credentials.password));
        }
        args.add("-url");
        args.add(String.valueOf(tableDataDir));
        args.add("-maxRecords");
        args.add(String.valueOf(settings.exportSettings.maxRecords));
        args.add("-maxConcurrentFiles");
        args.add(settings.exportSettings.maxConcurrentFiles);
        args.add("-maxConcurrentQueries");
        args.add(settings.exportSettings.maxConcurrentQueries);
        args.add("--schema.splits");
        args.add(settings.exportSettings.splits);
        args.add("-cl");
        args.add(String.valueOf(settings.exportSettings.consistencyLevel));
        args.add("-maxErrors");
        args.add("0");
        args.add("-header");
        args.add("true");
        args.add("--engine.executionId");
        args.add(operationId);
        args.add("-logDir");
        args.add(String.valueOf(settings.dsbulkLogDir));
        args.add("-query");
        args.add(buildExportQuery());
        args.add("--dsbulk.monitoring.console");
        args.add("false");
        args.add("--executor.maxPerSecond");
        args.add(String.valueOf(settings.maxRowsPerSecond));
        args.addAll(settings.exportSettings.extraDsbulkOptions);
        return args;
    }

    protected String escape(CqlIdentifier id) {
        return id.asCql(true);
    }

    protected String buildExportQuery() {
        StringBuilder builder = new StringBuilder("SELECT ");
        Iterator<ExportedColumn> cols = exportedTable.getColumns().iterator();
        while (cols.hasNext()) {
            ExportedColumn exportedColumn = cols.next();
            String name = escape(exportedColumn.col.getName());
            builder.append(name);
            if (cols.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(" FROM ");
        builder.append(escape(exportedTable.getKeyspace().getName()));
        builder.append(".");
        builder.append(escape(exportedTable.getTable().getName()));

        return builder.toString();
    }

    private ExportedTable buildExportedTable() {
        final ClusterInfo origin = this.settings.exportSettings.clusterInfo;
        final ExportSettings.ExportCredentials credentials = this.settings.exportSettings.credentials;
        try (CqlSession session = sessionFactory.newSession(origin, credentials)) {
            KeyspaceMetadata keyspace = session.getMetadata().getKeyspace(settings.keyspace).get();
            TableMetadata table = keyspace.getTable(this.settings.table).get();
            List<ExportedColumn> exportedColumns = buildExportedPKColumns(table);
            ExportedTable exportedTable = new ExportedTable(keyspace, table, exportedColumns);
            LOGGER.info("Table to migrate: {}", exportedTable );
            return exportedTable;
        }
    }


    private List<ExportedColumn> buildExportedPKColumns(
            TableMetadata table) {
        List<ExportedColumn> exportedColumns = new ArrayList<>();
        for (ColumnMetadata pk : table.getPrimaryKey()) {
            exportedColumns.add(new ExportedColumn(pk, true, null, null));
        }
        return exportedColumns;
    }
}

