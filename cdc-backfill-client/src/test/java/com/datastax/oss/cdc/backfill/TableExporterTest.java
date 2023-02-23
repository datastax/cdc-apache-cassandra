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

import com.datastax.oss.cdc.backfill.exporter.ExportSettings;
import com.datastax.oss.cdc.backfill.exporter.TableExporter;
import com.datastax.oss.cdc.backfill.factory.DsBulkFactory;
import com.datastax.oss.cdc.backfill.factory.SessionFactory;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.simulacron.server.BoundCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static com.datastax.oss.dsbulk.tests.utils.FileUtils.readAllLinesInDirectoryAsStream;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableExporterTest extends SimulacronITBase {
    private Path dataDir;
    private Path logsDir;
    private Path tableDir;
    private Path exportAck;
    private TableExporter exporter;
    private DsBulkFactory dsBulkFactory;
    private SessionFactory sessionFactory;
    private BackfillSettings settings;

    TableExporterTest(BoundCluster origin) {
        super(origin);
    }


    @BeforeAll
    void setLogConfigurationFile() throws URISyntaxException {
        // required because the embedded mode resets the logging configuration after
        // each DSBulk invocation.
        System.setProperty(
                "logback. configurationFile",
                Paths.get(getClass().getResource("/logback-test.xml").toURI()).toString());
    }

    @BeforeEach
    public void init() throws IOException {
        MockitoAnnotations.openMocks(this);

        // create temp dirs
        dataDir = Files.createTempDirectory("data");
        logsDir = Files.createTempDirectory("logs");
        tableDir = dataDir.resolve("test").resolve("t1");
        exportAck = dataDir.resolve("__exported__").resolve("test__t1.exported");

        sessionFactory = new SessionFactory();
        dsBulkFactory = new DsBulkFactory();
        settings = createSettings();
        exporter = new TableExporter(dsBulkFactory, sessionFactory, settings);
    }

    @AfterEach
    void deleteTempDirs() {
        if (dataDir != null && Files.exists(dataDir)) {
            FileUtils.deleteDirectory(dataDir);
        }
        if (logsDir != null && Files.exists(logsDir)) {
            FileUtils.deleteDirectory(logsDir);
        }
    }

    @Test
    public void testExportTable() throws IOException {
        final ExitStatus status = exporter.exportTable();
        assertEquals(status, ExitStatus.STATUS_OK);

        assertThat(tableDir).exists().isDirectory();
        assertThat(exportAck).exists().isRegularFile();
        assertThat(readAllLinesInDirectoryAsStream(tableDir).collect(toList()))
                .containsExactlyInAnyOrder("textPK", "first", "textPK", "second"); // 2 cvs files with headers
    }

    private BackfillSettings createSettings() {
        BackfillSettings settings = new BackfillSettings();
        settings.dataDir = dataDir;
        settings.keyspace = "test";
        settings.table = "t1";
        settings.exportSettings.clusterInfo = new ExportSettings.ExportClusterInfo();
        settings.exportSettings.clusterInfo.hostsAndPorts =
                Collections.singletonList(HostAndPort.fromString(originHost));
        settings.exportSettings.clusterInfo.protocolVersion = "V4";
        settings.dsbulkLogDir = logsDir;
        return settings;
    }
}
