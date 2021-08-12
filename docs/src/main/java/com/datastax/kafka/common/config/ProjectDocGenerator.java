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
package com.datastax.kafka.common.config;

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.common.sink.config.AsciiDocGenerator;
import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.config.SslConfig;

import java.io.IOException;
import java.nio.file.Paths;

public class ProjectDocGenerator {

    public static void generateCassourceSourceDocs(String outputDir) throws IOException {
        AsciiDocGenerator.generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraSource.adoc",
                "Cassandra Source Connector Configuration",
                CassandraSourceConnectorConfig.GLOBAL_CONFIG_DEF);

        AsciiDocGenerator.generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraAuth.adoc",
                "Cassandra Authentication Configuration",
                AuthenticatorConfig.CONFIG_DEF);

        AsciiDocGenerator.generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraSsl.adoc",
                "Cassandra SSL/TLS Configuration",
                SslConfig.CONFIG_DEF);
    }

    public static void main(String[] args) {
        try {
            ProjectDocGenerator.generateCassourceSourceDocs("docs/modules/ROOT/pages");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
