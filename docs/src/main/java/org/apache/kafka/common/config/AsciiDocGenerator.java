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
package org.apache.kafka.common.config;

import com.datastax.oss.cdc.CassandraSourceConnectorConfig;
import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.config.SslConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class AsciiDocGenerator {

    public String toAscidoc(ConfigDef configDef) {
        StringBuilder b = new StringBuilder();
        for (ConfigDef.ConfigKey key : sortedConfigs(configDef)) {
            if (key.internalConfig) {
                continue;
            }
            getConfigKeyAsciiDoc(configDef, key, b);
            b.append("\n");
        }
        return b.toString();
    }

    private void getConfigKeyAsciiDoc(ConfigDef configDef, ConfigDef.ConfigKey key, StringBuilder b) {
        b.append("[#").append(key.name).append("]").append("\n");
        for (String docLine : key.documentation.split("\n")) {
            if (docLine.length() == 0) {
                continue;
            }
            b.append(docLine).append("\n+\n");
        }
        b.append("Type: ").append(configDef.getConfigValue(key, "Type")).append("\n");
        if (key.hasDefault()) {
            b.append("Default: ").append(configDef.getConfigValue(key, "Default")).append("\n");
        }
        if (key.validator != null) {
            b.append("Valid Values: ").append(configDef.getConfigValue(key, "Valid Values")).append("\n");
        }
        b.append("Importance: ").append(configDef.getConfigValue(key, "Importance")).append("\n");
    }

    private List<ConfigDef.ConfigKey> sortedConfigs(ConfigDef configDef) {
        final Map<String, Integer> groupOrd = new HashMap<>(configDef.groups().size());
        int ord = 0;
        for (String group: configDef.groups()) {
            groupOrd.put(group, ord++);
        }

        List<ConfigDef.ConfigKey> configs = new ArrayList<>(configDef.configKeys().values());
        Collections.sort(configs, (k1, k2) -> compare(k1, k2, groupOrd));
        return configs;
    }

    private int compare(ConfigDef.ConfigKey k1, ConfigDef.ConfigKey k2, Map<String, Integer> groupOrd) {
        int cmp = k1.group == null
                ? (k2.group == null ? 0 : -1)
                : (k2.group == null ? 1 : Integer.compare(groupOrd.get(k1.group), groupOrd.get(k2.group)));
        if (cmp == 0) {
            cmp = Integer.compare(k1.orderInGroup, k2.orderInGroup);
            if (cmp == 0) {
                // first take anything with no default value
                if (!k1.hasDefault() && k2.hasDefault())
                    cmp = -1;
                else if (!k2.hasDefault() && k1.hasDefault())
                    cmp = 1;
                else {
                    cmp = k1.importance.compareTo(k2.importance);
                    if (cmp == 0)
                        return k1.name.compareTo(k2.name);
                }
            }
        }
        return cmp;
    }

    public void generateConfigDefDoc(Path path, String name, String title, ConfigDef configDef) throws IOException {
        try (FileWriter fileWriter = new FileWriter(path.resolve(name).toFile())) {
            PrintWriter pw = new PrintWriter(fileWriter);
            pw.append("= ").append(title).append("\n\n");
            pw.append("== Parameters").append("\n\n");
            pw.append(toAscidoc(configDef));
            pw.flush();
        }
    }

    public void generateCassourceSourceDocs(String outputDir) throws IOException {
        generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraSource.adoc",
                "Cassandra Source Connector Configuration",
                CassandraSourceConnectorConfig.GLOBAL_CONFIG_DEF);

        generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraAuth.adoc",
                "Cassandra Authentication Configuration",
                AuthenticatorConfig.CONFIG_DEF);

        generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraSsl.adoc",
                "Cassandra SSL/TLS Configuration",
                SslConfig.CONFIG_DEF);
    }

    public static void main(String[] args) {
        try {
            AsciiDocGenerator asciiDocGenerator = new AsciiDocGenerator();
            asciiDocGenerator.generateCassourceSourceDocs("docs/modules/ROOT/pages");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
