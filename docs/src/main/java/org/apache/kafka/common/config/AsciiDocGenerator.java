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

    protected static void getConfigKeyAsciiDoc(
            ConfigDef configDef, ConfigDef.ConfigKey key, StringBuilder b) {
        b.append("| *").append(key.name).append("*").append("\n");
        b.append("| ");
        for (String docLine : key.documentation.split("\n")) {
            if (docLine.length() == 0) {
                continue;
            }
            b.append(docLine);
        }
        b.append("\n| ").append(getConfigValue(key, "Type"));
        b.append("\n| ");
        if (key.validator != null) {
           b.append(getConfigValue(key, "Valid Values"));
        }
        b.append("\n| ");
        if (key.hasDefault()) {
            b.append(getConfigValue(key, "Default"));
        }
        //b.append("* Importance: ").append(getConfigValue(key, "Importance")).append("\n");
        b.append("\n");
    }

    protected static String getConfigValue(ConfigDef.ConfigKey key, String headerName) {
        switch (headerName) {
            case "Name":
                return key.name;
            case "Description":
                return key.documentation;
            case "Type":
                return key.type.toString().toLowerCase(Locale.ROOT);
            case "Default":
                if (key.hasDefault()) {
                    if (key.defaultValue == null) return "null";
                    String defaultValueStr = ConfigDef.convertToString(key.defaultValue, key.type);
                    if (defaultValueStr.isEmpty()) return "\"\"";
                    else return defaultValueStr;
                } else return "";
            case "Valid Values":
                return key.validator != null ? key.validator.toString() : "";
            case "Importance":
                return key.importance.toString().toLowerCase(Locale.ROOT);
            default:
                throw new RuntimeException(
                        "Can't find value for header '" + headerName + "' in " + key.name);
        }
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
            pw.append("== ").append(title).append("\n\n");
            pw.append(".Table ").append(title).append("\n")
                    .append("[cols=\"2,3,1,1,1\"]\n")
                    .append("|===\n")
                    .append("|Name | Description | Type | Validator | Default\n\n");
            pw.append(toAscidoc(configDef));
            pw.append("|===\n");
            pw.flush();
        }
    }

    public void generateCassourceSourceDocs(String outputDir) throws IOException {
        generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraSource.adoc",
                "Cassandra Source Connector settings",
                CassandraSourceConnectorConfig.GLOBAL_CONFIG_DEF);

        generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraAuth.adoc",
                "Cassandra Authentication settings",
                AuthenticatorConfig.CONFIG_DEF);

        generateConfigDefDoc(Paths.get(outputDir),
                "cfgCassandraSsl.adoc",
                "Cassandra SSL/TLS settings",
                SslConfig.CONFIG_DEF);
    }

    public static void main(String[] args) {
        try {
            String targetDir = args.length == 1 ? args[0] : "docs/modules/ROOT/pages";
            System.out.println("Generating connector settings documentation in " + targetDir);
            AsciiDocGenerator asciiDocGenerator = new AsciiDocGenerator();
            asciiDocGenerator.generateCassourceSourceDocs(targetDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
