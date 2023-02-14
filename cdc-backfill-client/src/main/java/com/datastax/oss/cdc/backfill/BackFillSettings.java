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

import picocli.CommandLine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class BackFillSettings {

    @CommandLine.Option(
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Displays this help message.")
    boolean usageHelpRequested;

    @CommandLine.Option(
            names = {"-d", "--data-dir"},
            paramLabel = "PATH",
            description =
                    "The directory where data will be exported to and imported from."
                            + "The default is a 'data' subdirectory in the current working directory. "
                            + "The data directory will be created if it does not exist. "
                            + "Tables will be exported and imported in subdirectories of the data directory specified here; "
                            + "there will be one subdirectory per keyspace inside the data directory, "
                            + "then one subdirectory per table inside each keyspace directory.",
            defaultValue = "data")
    public Path dataDir = Paths.get("data");

    @CommandLine.Option(
            names = {"-k", "--keyspace"},
            required = true,
            description =
                    "The name of the keyspace where the table to be exported exist")
    public String keyspace;

    @CommandLine.Option(
            names = {"-t", "--table"},
            required = true,
            description =
                    "The name of the table to export data from for cdc back filling")
    public String table;


    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
    public ExportSettings exportSettings = new ExportSettings();

    @CommandLine.Option(
            names = {"-c", "--dsbulk-cmd"},
            paramLabel = "CMD",
            description =
                    "The external DSBulk command to use. Ignored if the embedded DSBulk is being used. "
                            + "The default is simply 'dsbulk', assuming that the command is available through the "
                            + "PATH variable contents.",
            defaultValue = "dsbulk")
    public String dsbulkCmd = "dsbulk";

    @CommandLine.Option(
            names = {"-l", "--dsbulk-log-dir"},
            paramLabel = "PATH",
            description =
                    "The directory where DSBulk should store its logs. "
                            + "The default is a 'logs' subdirectory in the current working directory. "
                            + "This subdirectory will be created if it does not exist. "
                            + "Each DSBulk operation will create a subdirectory inside the log directory specified here.",
            defaultValue = "logs")
    public Path dsbulkLogDir = Paths.get("logs");

    @CommandLine.Option(
            names = {"-w", "--dsbulk-working-dir"},
            paramLabel = "PATH",
            description =
                    "The directory where DSBulk should be executed. "
                            + "Ignored if the embedded DSBulk is being used. "
                            + "If unspecified, it defaults to the current working directory.")
    public Path dsbulkWorkingDir;

    @CommandLine.Option(
            names = "--max-concurrent-ops",
            paramLabel = "NUM",
            description =
                    "The maximum number of concurrent operations (exports and imports) to carry. Default is 1. "
                            + "Set this to higher values to allow exports and imports to occur concurrently; "
                            + "e.g. with a value of 2, each table will be imported as soon as it is exported, "
                            + "while the next table is being exported.",
            defaultValue = "1")
    public int maxConcurrentOps = 1;
}
