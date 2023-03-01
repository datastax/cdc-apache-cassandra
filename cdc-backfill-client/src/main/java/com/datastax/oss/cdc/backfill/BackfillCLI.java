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

import com.datastax.oss.cdc.backfill.exporter.TableExporter;
import com.datastax.oss.cdc.backfill.factory.BackfillFactory;
import com.datastax.oss.cdc.backfill.factory.ConnectorFactory;
import com.datastax.oss.cdc.backfill.importer.PulsarImporter;
import com.datastax.oss.cdc.backfill.util.LoggingUtils;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URISyntaxException;

@Command(
        name = "BackfillCLI",
        description =
                "A tool for back-filling the CDC data topic with historical data from that source Cassandra table.",
        versionProvider = VersionProvider.class,
        sortOptions = false,
        usageHelpWidth = 100)
public class BackfillCLI {

    @Option(
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Displays this help message.")
    boolean usageHelpRequested;

    @Option(
            names = {"-v", "--version"},
            versionHelp = true,
            description = "Displays version info.")
    boolean versionInfoRequested;

    public static void main(String[] args) {
        LoggingUtils.configureLogging(LoggingUtils.MIGRATOR_CONFIGURATION_FILE);
        CommandLine commandLine = new CommandLine(new BackfillCLI());
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }

    @Command(
            name = "backfill",
            optionListHeading = "Available options:%n",
            abbreviateSynopsis = true,
            usageHelpWidth = 100)
    private int backfill(
            @ArgGroup(exclusive = false, multiplicity = "1") BackfillSettings settings) throws URISyntaxException, IOException {
        // Bootstrap the backfill dependencies
        final BackfillFactory factory = new BackfillFactory(settings);
        final TableExporter exporter = factory.newTableExporter();
        final ConnectorFactory connectorFactory = new ConnectorFactory(exporter.getTableDataDir());
        final PulsarImporter importer = factory.newPulsarImporter(connectorFactory, exporter.getExportedTable());
        final CassandraToPulsarMigrator migrator = new CassandraToPulsarMigrator(exporter, importer);

        return migrator.migrate().exitCode();
    }
}

