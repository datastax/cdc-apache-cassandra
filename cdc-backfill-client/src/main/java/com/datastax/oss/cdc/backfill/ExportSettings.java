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

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ExportSettings {

    @ArgGroup(multiplicity = "1")
    public ExportClusterInfo clusterInfo;

    public static class ExportClusterInfo implements ClusterInfo {

        @Option(
                names = "--export-host",
                paramLabel = "HOST[:PORT]",
                description =
                        "The host name or IP and, optionally, the port of a node from the origin cluster. "
                                + "If the port is not specified, it will default to 9042. "
                                + "This option can be specified multiple times. "
                                + "Options --export-host and --export-bundle are mutually exclusive.",
                converter = HostAndPortConverter.class,
                required = true)
        public List<HostAndPort> hostsAndPorts;

        @Option(
                names = "--export-bundle",
                paramLabel = "PATH",
                description =
                        "The path to a secure connect bundle to connect to the origin cluster, "
                                + "if that cluster is a DataStax Astra cluster. "
                                + "Options --export-host and --export-bundle are mutually exclusive.",
                required = true)
        public Path bundle;

        @Option(
                names = "--export-protocol-version",
                paramLabel = "VERSION",
                description =
                        "The protocol version to use to connect to the origin cluster, e.g. 'V4'. "
                                + "If not specified, the driver will negotiate the highest version supported by both "
                                + "the client and the server.")
        public String protocolVersion;

        @Override
        public boolean isOrigin() {
            return true;
        }

        @Override
        public List<InetSocketAddress> getContactPoints() {
            if (hostsAndPorts == null || hostsAndPorts.isEmpty()) {
                return Collections.emptyList();
            }
            return hostsAndPorts.stream()
                    .map(
                            hp ->
                                    InetSocketAddress.createUnresolved(
                                            hp.getHost(), hp.hasPort() ? hp.getPort() : 9042))
                    .collect(Collectors.toList());
        }

        @Override
        public Path getBundle() {
            return bundle;
        }

        @Override
        public String getProtocolVersion() {
            return protocolVersion;
        }
    }

    @ArgGroup(exclusive = false)
    public ExportCredentials credentials;

    public static class ExportCredentials implements Credentials {

        @Option(
                names = "--export-username",
                paramLabel = "STRING",
                description =
                        "The username to use to authenticate against the origin cluster. "
                                + "Options --export-username and --export-password must be provided together, or not at all.",
                required = true)
        public String username;

        @Option(
                names = "--export-password",
                paramLabel = "STRING",
                description =
                        "The password to use to authenticate against the origin cluster. "
                                + "Options --export-username and --export-password must be provided together, or not at all. "
                                + "Omit the parameter value to be prompted for the password interactively.",
                required = true,
                prompt = "Enter the password to use to authenticate against the origin cluster: ",
                interactive = true)
        public char[] password;

        @Override
        public String getUsername() {
            return username;
        }

        @Override
        public char[] getPassword() {
            return password;
        }
    }

    @Option(
            names = "--export-consistency",
            paramLabel = "CONSISTENCY",
            description =
                    "The consistency level to use when exporting data. The default is LOCAL_QUORUM.",
            defaultValue = "LOCAL_QUORUM")
    public DefaultConsistencyLevel consistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;

    @Option(
            names = "--export-max-records",
            paramLabel = "NUM",
            description =
                    "The maximum number of records to export for each table. Must be a positive number or -1. "
                            + "The default is -1 (export the entire table).",
            defaultValue = "-1")
    public int maxRecords = -1;

    @Option(
            names = "--export-max-concurrent-files",
            paramLabel = "NUM|AUTO",
            description =
                    "The maximum number of concurrent files to write to. "
                            + "Must be a positive number or the special value AUTO. The default is AUTO.",
            defaultValue = "AUTO")
    public String maxConcurrentFiles = "AUTO";

    @Option(
            names = "--export-max-concurrent-queries",
            paramLabel = "NUM|AUTO",
            description =
                    "The maximum number of concurrent queries to execute. "
                            + "Must be a positive number or the special value AUTO. The default is AUTO.",
            defaultValue = "AUTO")
    public String maxConcurrentQueries = "AUTO";

    @Option(
            names = "--export-splits",
            paramLabel = "NUM|NC",
            description =
                    "The maximum number of token range queries to generate. "
                            + "Use the NC syntax to specify a multiple of the number of available cores, "
                            + "e.g. 8C = 8 times the number of available cores. The default is 8C. "
                            + "This is an advanced setting; you should rarely need to modify the default value.",
            defaultValue = "8C")
    public String splits = "8C";

    @Option(
            names = "--export-dsbulk-option",
            paramLabel = "OPT=VALUE",
            description =
                    "An extra DSBulk option to use when exporting. "
                            + "Any valid DSBulk option can be specified here, and it will passed as is to the DSBulk process. "
                            + "DSBulk options, including driver options, must be passed as '--long.option.name=<value>'. "
                            + "Short options are not supported. ")
    public List<String> extraDsbulkOptions = new ArrayList<>();
}
