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

package com.datastax.oss.cdc.backfill.factory;

import com.datastax.oss.cdc.backfill.BackfillSettings;
import com.datastax.oss.cdc.backfill.exporter.ExportedTable;
import com.datastax.oss.cdc.backfill.importer.PulsarImporter;
import com.datastax.oss.cdc.backfill.exporter.TableExporter;
import com.datastax.oss.cdc.backfill.util.ConnectorUtils;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

public class BackfillFactory {
    private final BackfillSettings settings;

    public BackfillFactory(BackfillSettings setting) {
        this.settings = setting;
    }

    public TableExporter newTableExporter() {
        // export from C* table to disk
        return new TableExporter(new DsBulkFactory(), new SessionFactory(), settings);
    }

    public Connector newCVSConnector(final Path tableDataDir) throws URISyntaxException, IOException {
        CSVConnector connector = new CSVConnector();
        Config connectorConfig =
                ConnectorUtils.createConfig(
                        "dsbulk.connector.csv",
                        "url",
                        tableDataDir,
                        "recursive",
                        true,
                        "fileNamePattern",
                        "\"**/output-*\"");
        connector.configure(connectorConfig, true, true);
        return connector;
    }

    public PulsarImporter createPulsarImporter(Connector connector, ExportedTable exportedTable) {
        return new PulsarImporter(connector, exportedTable,
                new PulsarMutationSenderFactory(settings.importSettings));
    }
}
