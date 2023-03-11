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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BackfillCommand implements CustomCommand {
    @Override
    public String name() {
        return "backfill";
    }

    @Override
    public String description() {
        return "Backfills the CDC data topic with historical data from that source Cassandra table.";
    }

    @Override
    public List<ParameterDescriptor> parameters() {
        List<ParameterDescriptor> parameters = new ArrayList<>();
        parameters.add(
                ParameterDescriptor.builder()
                        .description("The directory where data will be exported to and imported from")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--data-dir", "-d"))
                        .required(false)
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The host name or IP and, optionally, the port of a node from the origin cluster. " +
                                "If the port is not specified, it will default to 9042.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-host"))
                        .required(true)
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The username to use to authenticate against the origin cluster.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-username"))
                        .required(true)
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description(
                                "The password to use to authenticate against the origin cluster.")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--export-password"))
                        .required(true)
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description("The name of the keyspace where the table to be exported exists")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--keyspace", "-k"))
                        .required(true)
                        .build());
        parameters.add(
                ParameterDescriptor.builder()
                        .description("The name of the table to export data from for cdc back filling")
                        .type(ParameterType.STRING)
                        .names(Arrays.asList("--table", "-t"))
                        .required(true)
                        .build());
        return parameters;
    }

    @Override
    public boolean execute(Map<String, Object> parameters, CommandExecutionContext context) {
        CommandLine commandLine = new CommandLine(new BackfillCLI());
        List<String> args = parameters.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.toList());
        args.add(0, "backfill");
        //args.add("--export-dsbulk-option='-f=META-INF/resources/reference.conf'");

        System.out.println("Executing: " + args);
        int exitCode = commandLine.execute(args.toArray(new String[0]));
        return exitCode == 0;
    }
}
