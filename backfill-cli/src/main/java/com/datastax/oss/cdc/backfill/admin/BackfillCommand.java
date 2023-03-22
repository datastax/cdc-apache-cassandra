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

package com.datastax.oss.cdc.backfill.admin;

import com.datastax.oss.cdc.backfill.BackfillCLI;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class BackfillCommand implements CustomCommand {
    @Override
    public String name() {
        return "backfill";
    }

    @Override
    public String description() {
        return "Backfills the CDC data topic with historical data from that source Cassandra table.";
    }

    /**
     * Delegate to the picocli library to validate the command line arguments so all parameters are marked as optional.
     * Commands have to be defined here to be accepted by the Pulsar CLI extension but the idea is to make them
     * as thin as possible
     */
    @Override
    public List<ParameterDescriptor> parameters() {
        List<ParameterDescriptor> parameters = new ArrayList<>();
        parameters.add(
                ParameterDescriptor.builder()
                        .description("The directory where data will be exported to and imported from")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--data-dir", "-d"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The host name or IP and, optionally, the port of a node from the Cassandra cluster. " +
                                "If the port is not specified, it will default to 9042.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-host"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The path to a secure connect bundle to connect to the Cassandra cluster, "
                                        + "if that cluster is a DataStax Astra cluster. "
                                        + "Options --export-host and --export-bundle are mutually exclusive.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-bundle"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The protocol version to use to connect to the Cassandra cluster, e.g. 'V4'. "
                                        + "If not specified, the driver will negotiate the highest version supported by both "
                                        + "the client and the server.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-protocol-version"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The username to use to authenticate against the origin cluster.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-username"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The password to use to authenticate against the origin cluster.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-password"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The consistency level to use when exporting data. The default is LOCAL_QUORUM.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-consistency"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The maximum number of records to export from the table. Must be a positive number or -1. "
                                        + "The default is -1 (export the entire table).")
                        .type(ParameterType.INTEGER)
                        .names(Arrays.asList("--export-max-records"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The maximum number of concurrent files to write to. "
                                        + "Must be a positive number or the special value AUTO. The default is AUTO.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-max-concurrent-files"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The maximum number of concurrent queries to execute. "
                                        + "Must be a positive number or the special value AUTO. The default is AUTO.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-max-concurrent-queries"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "An extra DSBulk option to use when exporting. "
                                        + "Any valid DSBulk option can be specified here, and it will passed as is to the DSBulk process. "
                                        + "DSBulk options, including driver options, must be passed as '--long.option1.name=<value1>|--long.option2.name=<value2>'. "
                                        + "Short options are not supported. ")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-dsbulk-option"))
                        .build());

        parameters.add(
                ParameterDescriptor.builder()
                        .description("The name of the keyspace where the table to be exported exists")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--keyspace", "-k"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description("The name of the table to export data from for cdc back filling")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--table", "-t"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description("The event topic name prefix. The `<keyspace_name>.<table_name>` is appended to that prefix to build the topic name. "
                                + "The default value is `events-`.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--events-topic-prefix"))
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description( "The maximum number of rows per second to read from the Cassandra table. "
                                + "Setting this option to any negative value or zero will disable it. The default is -1.")
                        .type(ParameterType.INTEGER)
                        .names(Arrays.asList("--max-rows-per-second"))
                        .build());

        return parameters;
    }

    @Override
    public boolean execute(Map<String, Object> parameters, CommandExecutionContext context) {
        List<String> args = parseParameters(parameters, context);
        int exitCode = new CommandLine(new BackfillCLI())
                .setParameterExceptionHandler(new ShortErrorMessageHandler())
                .execute(args.toArray(new String[0]));
        return exitCode == 0;
    }

    private List<String> parseParameters(Map<String, Object> parameters,  CommandExecutionContext context) {
        List<String> args = parameters.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .flatMap(e -> explodeListOptions(e))
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.toList());
        PulsarClientParams params = parseClientConfRootParams(context.getConfiguration());
        populateClientConfRootParams(args, params);
        return args;
    }

    private static Set<String> listOptions = new HashSet<>(Arrays.asList("--export-dsbulk-option"));
    private Stream<? extends Map.Entry<String,Object>> explodeListOptions(Map.Entry<String, Object> entry) {
        List<Map.Entry<String, Object>> entries = new ArrayList<>();
        if (listOptions.contains(entry.getKey())) {
            String[] options = entry.getValue().toString().split("\\|");
            for (String option : options) {
                entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), option));
            }
        } else {
            entries.add(entry);
        }
        return entries.stream();
    }

    private void populateClientConfRootParams(List<String> args, PulsarClientParams params) {
        if (isNotBlank(params.serviceURL)) {
            args.add("--pulsar-url=" + params.serviceURL);
        }
        if (isNotBlank(params.authParams)) {
            args.add("--pulsar-auth-params=" + params.authParams);
        }
        if (isNotBlank(params.tlsProvider)) {
            args.add("--pulsar-ssl-provider=" + params.tlsProvider);
        }
        if (isNotBlank(params.tlsTrustStorePath)) {
            args.add("--sslTruststorePath=" + params.tlsTrustStorePath);
        }
        if (isNotBlank(params.tlsTrustStorePassword)) {
            args.add("--pulsar-ssl-truststore-password=" + params.tlsTrustStorePassword);
        }
        if (isNotBlank(params.tlsTrustStoreType)) {
            args.add("--pulsar-ssl-truststore-type=" + params.tlsTrustStoreType);
        }
        if (isNotBlank(params.tlsKeyStorePath)) {
            args.add("--pulsar-ssl-keystore-path=" + params.tlsKeyStorePath);
        }
        if (isNotBlank(params.tlsKeyStorePassword)) {
            args.add("--pulsar-ssl-keystore-password=" + params.tlsKeyStorePassword);
        }
        if (isNotBlank(params.tlsCiphers)) {
            args.add("--pulsar-ssl-cipher-suites=" + params.tlsCiphers);
        }
        if (isNotBlank(params.tlsProtocols)) {
            args.add("--pulsar-ssl-enabled-protocols=" + params.tlsProtocols);
        }
        args.add("--pulsar-ssl-allow-insecure-connections=" + params.tlsAllowInsecureConnection);
        args.add("--pulsar-ssl-enable-hostname-verification=" + params.tlsEnableHostnameVerification);
        if(isNotBlank(params.tlsTrustCertsFilePath)) {
            args.add("--pulsar-ssl-tls-trust-certs-path=" + params.tlsTrustCertsFilePath);
        }
        args.add("--pulsar-ssl-use-key-store-tls=" + params.useKeyStoreTls);
        if(isNotBlank(params.authPluginClassName)) {
            args.add("--pulsar-auth-plugin-class-name=" + params.authPluginClassName);
        }
    }

    /**
     * Wraps the configs that are used to initialize the Pulsar client.
     */
    private static class PulsarClientParams {
        String serviceURL;
        String authPluginClassName;
        String authParams;
        String tlsProvider;
        boolean useKeyStoreTls;
        String tlsTrustStoreType;
        String tlsTrustStorePath;
        String tlsTrustStorePassword;
        String tlsKeyStoreType;
        String tlsKeyStorePath;
        String tlsKeyStorePassword;
        String tlsCiphers;
        String tlsProtocols;
        String tlsKeyFilePath;
        String tlsCertificateFilePath;
        boolean tlsAllowInsecureConnection;
        boolean tlsEnableHostnameVerification;
        String tlsTrustCertsFilePath;
    }

    private static PulsarClientParams parseClientConfRootParams(Properties properties) {
        PulsarClientParams params = new PulsarClientParams();
        params.serviceURL = isNotBlank(properties.getProperty("brokerServiceUrl"))
                ? properties.getProperty("brokerServiceUrl") : properties.getProperty("webServiceUrl");
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        if (isBlank(params.serviceURL)) {
            params.serviceURL = properties.getProperty("serviceUrl");
        }
        params.authPluginClassName = properties.getProperty("authPlugin");
        params.authParams = properties.getProperty("authParams");
        params.tlsProvider = properties.getProperty("webserviceTlsProvider");

        params.useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        params.tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        params.tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        params.tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");
        params.tlsKeyStoreType = properties.getProperty("tlsKeyStoreType", "JKS");
        params.tlsKeyStorePath = properties.getProperty("tlsKeyStorePath");
        params.tlsKeyStorePassword = properties.getProperty("tlsKeyStorePassword");
        params.tlsCiphers = properties.getProperty("tlsCiphers");
        params.tlsProtocols = properties.getProperty("tlsProtocols");
        params.tlsKeyFilePath = properties.getProperty("tlsKeyFilePath");
        params.tlsCertificateFilePath = properties.getProperty("tlsCertificateFilePath");

        params.tlsAllowInsecureConnection = Boolean.parseBoolean(properties
                .getProperty("tlsAllowInsecureConnection", "false"));

        params.tlsEnableHostnameVerification = Boolean.parseBoolean(properties
                .getProperty("tlsEnableHostnameVerification", "false"));
        params.tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");
        return params;
    }


    /**
     * Shorten the error message by excluding the usage help message. This makes it more consistent with pulsar admin
     * when invalid options are used.
     */
    static class ShortErrorMessageHandler implements CommandLine.IParameterExceptionHandler {
        public int handleParseException(CommandLine.ParameterException ex, String[] args) {
            CommandLine cmd = ex.getCommandLine();
            PrintWriter writer = cmd.getErr();

            writer.println(ex.getMessage());
            CommandLine.UnmatchedArgumentException.printSuggestions(ex, writer);
            writer.print(cmd.getHelp().fullSynopsis());

            CommandLine.Model.CommandSpec spec = cmd.getCommandSpec();
            writer.printf("Try '%s' for more information.%n", "cassandra-cdc");

            return cmd.getExitCodeExceptionMapper() != null
                    ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
                    : spec.exitCodeOnInvalidInput();
        }
    }
}
