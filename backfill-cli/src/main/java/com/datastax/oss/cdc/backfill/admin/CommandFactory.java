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

import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;

import java.util.Arrays;
import java.util.List;

public class CommandFactory implements CustomCommandFactory {

    @Override
    public List<CustomCommandGroup> commandGroups(CommandExecutionContext context) {
        return Arrays.asList(new CDCCommandGroup());
    }

    private static class CDCCommandGroup implements CustomCommandGroup {
        @Override
        public String name() {
            return "cassandra-cdc";
        }

        @Override
        public String description() {
            return "Cassandra CDC commands";
        }

        @Override
        public List<CustomCommand> commands(CommandExecutionContext context) {
            return Arrays.asList(
                    new BackfillCommand());
        }
    }
}
