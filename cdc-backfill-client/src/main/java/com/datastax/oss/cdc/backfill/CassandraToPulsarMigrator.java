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

import com.datastax.oss.cdc.backfill.util.ConnectorUtils;
import com.datastax.oss.cdc.backfill.util.ModelUtils;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class CassandraToPulsarMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToPulsarMigrator.class);

    private static final Object POISON_PILL = new Object();

    private final BackFillSettings settings;
    private final BlockingQueue<TableExporter> exportQueue;
    private final BlockingQueue<Object> importQueue;
    private final ExecutorService pool;

    private final CopyOnWriteArrayList<TableExportReport> successful =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<TableExportReport> failed = new CopyOnWriteArrayList<>();

    private final TableExporter exporter;

    private final PulsarImporter importer;

    public CassandraToPulsarMigrator(BackFillSettings settings) {
        this.settings = settings;
        exportQueue = new LinkedBlockingQueue<>();
        importQueue = new LinkedBlockingQueue<>();
        pool = Executors.newFixedThreadPool(settings.maxConcurrentOps);
        final ExportedTable table = ModelUtils.buildExportedTable(
                settings.exportSettings.clusterInfo, settings.exportSettings.credentials, settings);
        // export from C* table to disk
        exporter = new TableExporter(table, settings);
        CSVConnector connector = new CSVConnector();
        Config connectorConfig =
                ConnectorUtils.createConfig(
                        "dsbulk.connector.csv",
                        "url",
                        exporter.tableDataDir,
                        "recursive",
                        true,
                        "fileNamePattern",
                        "\"**/output-*\"");
        connector.configure(connectorConfig, true, true);

        // import from disk to Pulsar topic
        importer = new PulsarImporter(connector, table);
    }

    public ExitStatus migrate() {
        try {
            migrateTables();
            exportQueue.clear();
            importQueue.clear();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            pool.shutdownNow();
            LOGGER.info(
                    "Migration finished with {} successfully migrated tables, {} failed tables.",
                    successful.size(),
                    failed.size());
            for (TableExportReport report : successful) {
                LOGGER.info("Table {} migrated successfully.", report.getMigrator().getExportedTable());
            }
            for (TableExportReport report : failed) {
                LOGGER.error(
                        "Table {} could not be {}: operation {} exited with {}.",
                        report.getMigrator().getExportedTable(),
                        report.isExport() ? "exported" : "imported",
                        report.getOperationId(),
                        report.getStatus());
            }
        }
        if (failed.isEmpty()) {
            return ExitStatus.STATUS_OK;
        }
        if (successful.isEmpty()) {
            return ExitStatus.STATUS_ABORTED_TOO_MANY_ERRORS;
        }
        return ExitStatus.STATUS_COMPLETED_WITH_ERRORS;
    }

    private void migrateTables() {
        exportQueue.add(exporter);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.add(CompletableFuture.runAsync(this::exportTables, pool));
        futures.add(CompletableFuture.runAsync(this::importTables, pool));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
    }

    private void exportTables() {
        TableExporter migrator;
        while ((migrator = exportQueue.poll()) != null) {
            TableExportReport report;
            try {
                report = migrator.exportTable();
            } catch (Exception e) {
                LOGGER.error(
                        "Table "
                                + migrator.getExportedTable()
                                + ": unexpected error when exporting data, aborting",
                        e);
                report =
                        new TableExportReport(migrator, ExitStatus.STATUS_ABORTED_FATAL_ERROR, null, true);
            }
            if (report.getStatus() == ExitStatus.STATUS_OK) {
                importQueue.add(migrator);
            } else {
                failed.add(report);
            }
        }
        importQueue.add(POISON_PILL);
    }

    private void importTables() {
        // Output useful logs
        while (true) {
            TableExporter migrator;
            try {
                Object o = importQueue.take();
                if (o == POISON_PILL) {
                    break;
                }
                migrator = (TableExporter) o;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            TableExportReport report;
            try {
                report = importer.importTable(migrator); // TODO: Decouple status reports between Exporter & Importer
            } catch (Exception e) {
                LOGGER.error(
                        "Table "
                                + migrator.getExportedTable()
                                + ": unexpected error when importing data, aborting",
                        e);
                report =
                        new TableExportReport(migrator, ExitStatus.STATUS_ABORTED_FATAL_ERROR, null, false);
            }
            if (report.getStatus() == ExitStatus.STATUS_OK) {
                successful.add(report);
            } else {
                failed.add(report);
            }
        }
    }
}
