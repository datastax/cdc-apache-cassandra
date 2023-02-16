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

import com.clearspring.analytics.stream.membership.DataOutputBuffer;
import com.datastax.oss.cdc.agent.AgentConfig;
import com.datastax.oss.cdc.agent.Mutation;
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.cdc.backfill.util.LoggingUtils;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.DefaultIndexedField;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.datastax.oss.cdc.agent.AgentConfig.PULSAR_SERVICE_URL;

public class TableExporter extends TableProcessor {
    private static final URL DSBULK_CONFIGURATION_FILE =
            Objects.requireNonNull(ClassLoader.getSystemResource("logback-dsbulk-embedded.xml"));
    private static final Logger LOGGER = LoggerFactory.getLogger(TableExporter.class);

    protected final BackFillSettings settings;

    protected final Path tableDataDir;

    protected final Path exportAckDir;
    protected final Path exportAckFile;

    protected final Path importAckDir;
    protected final Path importAckFile;

    public TableExporter(ExportedTable exportedTable, BackFillSettings settings) {
        super(exportedTable);
        this.settings = settings;
        this.tableDataDir =
                settings
                        .dataDir
                        .resolve(exportedTable.keyspace.getName().asInternal())
                        .resolve(exportedTable.table.getName().asInternal());
        this.exportAckDir = settings.dataDir.resolve("__exported__");
        this.importAckDir = settings.dataDir.resolve("__imported__");
        this.exportAckFile =
                exportAckDir.resolve(
                        exportedTable.keyspace.getName().asInternal()
                                + "__"
                                + exportedTable.table.getName().asInternal()
                                + ".exported");
        this.importAckFile =
                importAckDir.resolve(
                        exportedTable.keyspace.getName().asInternal()
                                + "__"
                                + exportedTable.table.getName().asInternal()
                                + ".imported");
    }

    public TableExportReport exportTable() {
        String operationId;
        if ((operationId = retrieveExportOperationId()) != null) {
            LOGGER.warn(
                    "Table {}.{}: already exported, skipping (delete this file to re-export: {}).",
                    exportedTable.keyspace.getName(),
                    exportedTable.table.getName(),
                    exportAckFile);
            return new TableExportReport(this, ExitStatus.STATUS_OK, operationId, true);
        } else {
            LOGGER.info("Exporting {}...", exportedTable.fullyQualifiedName);
            operationId = createOperationId(true);
            List<String> args = createExportArgs(operationId);
            ExitStatus status = invokeDsbulk(operationId, args);
            LOGGER.info("Export of {} finished with {}", exportedTable.fullyQualifiedName, status);
            if (status == ExitStatus.STATUS_OK) {
                createExportAckFile(operationId);
            }
            return new TableExportReport(this, status, operationId, true);
        }
    }
    
    public boolean isExported() {
        return Files.exists(exportAckFile);
    }

    public boolean isImported() {
        return Files.exists(importAckFile);
    }

    public ExitStatus invokeDsbulk(String operationId, List<String> args) {
        DataStaxBulkLoader loader = new DataStaxBulkLoader(args.toArray(new String[0]));
        int exitCode;
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
    private String createOperationId(boolean export) {
        ZonedDateTime now = Instant.now().atZone(ZoneOffset.UTC);
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS").format(now);
        return String.format(
                "%s_%s_%s_%s",
                (export ? "EXPORT" : "IMPORT"),
                exportedTable.keyspace.getName().asInternal(),
                exportedTable.table.getName().asInternal(),
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

    private void createImportAckFile(String operationId) {
        try {
            Files.createDirectories(importAckDir);
            Files.createFile(importAckFile);
            Files.write(importAckFile, operationId.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean hasExportedData() {
        if (isExported()) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tableDataDir)) {
                for (Path entry : stream) {
                    String fileName = entry.getFileName().toString();
                    if (fileName.startsWith("output")) {
                        return true;
                    }
                }
            } catch (IOException ignored) {
            }
        }
        return false;
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
        args.addAll(settings.exportSettings.extraDsbulkOptions);
        return args;
    }

    private List<String> createImportArgs(String operationId) {
        List<String> args = new ArrayList<>();
        args.add("load");
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
        args.add("false");
        args.add("--engine.executionId");
        args.add(operationId);
        args.add("-logDir");
        args.add(String.valueOf(settings.dsbulkLogDir));
        args.add("-query");
        args.add(buildExportQuery());
        args.add("--dsbulk.monitoring.console");
        args.add("false");
        args.addAll(settings.exportSettings.extraDsbulkOptions);
        return args;
    }

    @Override
    protected String escape(String text) {
        return text.replace("\"", "\\\"");
    }
}

