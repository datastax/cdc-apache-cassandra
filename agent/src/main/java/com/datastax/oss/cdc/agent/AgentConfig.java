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
package com.datastax.oss.cdc.agent;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
public class AgentConfig {

    public static final String CDC_PROPERTY_PREFIX = "cdc.";
    public static final String storageDir = System.getProperty("cassandra.storagedir", null);

    public enum Platform {
        ALL, PULSAR
    }

    @AllArgsConstructor
    public static class Setting<T> {
        public final String name;
        public final Platform platform;
        public final BiFunction<AgentConfig, String, T> initializer;
        public final Function<AgentConfig, T> supplier;
        public final String documentation;

        // for doc only
        public final T defaultValue;
        public final String type;
        public final String group;
        public final int orderInGroup;

        protected void getAsciiDoc(StringBuilder b) {
            b.append("| *").append(name).append("*").append("\n");
            b.append("| ");
            for (String docLine : documentation.split("\n")) {
                if (docLine.length() == 0) {
                    continue;
                }
                b.append(docLine).append("\n");
            }
            //b.append("Platform: ").append(getConfigValue("Platform")).append("\n");
            b.append("| ").append(getConfigValue("Type")).append("\n");
            b.append("| ");
            if (defaultValue != null) {
                b.append(getConfigValue("Default")).append("\n");
            }
            b.append("\n");
        }

        protected String getConfigValue(String headerName) {
            switch (headerName) {
                case "Name":
                    return name;
                case "Description":
                    return documentation;
                case "Platform":
                    return platform.name();
                case "Type":
                    return type.toLowerCase(Locale.ROOT);
                case "Default":
                    return (defaultValue != null) ? defaultValue.toString() : "";
                default:
                    throw new RuntimeException("Can't find value for header '" + headerName + "' in " + name);
            }
        }
    }

    public static final String TOPIC_PREFIX = "topicPrefix";
    public String topicPrefix = System.getProperty(CDC_PROPERTY_PREFIX + TOPIC_PREFIX, "events-");
    public static final Setting<String> TOPIC_PREFIX_SETTING =
            new Setting<>(TOPIC_PREFIX, Platform.ALL, (c, s) -> c.topicPrefix = s, c -> c.topicPrefix,
                    "The event topic name prefix. The <keyspace_name>.<table_name> is appended to that prefix to build the topic name.",
                    "events-", "String",
                    "main", 1);

    public static final String CDC_WORKING_DIR = "cdcWorkingDir";
    public String cdcWorkingDir = System.getProperty(CDC_PROPERTY_PREFIX + CDC_WORKING_DIR, storageDir + File.separator + "cdc");
    public static final Setting<String> CDC_RELOCATION_DIR_SETTING =
            new Setting<>(CDC_WORKING_DIR, Platform.ALL, (c, s) -> c.cdcWorkingDir = s, c -> c.cdcWorkingDir,
                    "The CDC working directory where the last sent offset is saved, and where the archived and errored commitlogs files are copied.",
                    "cdc", "String",
                    "main", 2);

    public static final String CDC_DIR_POLL_INTERVAL_MS = "cdcPollIntervalMs";
    public long cdcDirPollIntervalMs = Long.getLong(CDC_PROPERTY_PREFIX + CDC_DIR_POLL_INTERVAL_MS, 60000L);
    public static final Setting<Long> CDC_DIR_POLL_INTERVAL_MS_SETTING =
            new Setting<>(CDC_DIR_POLL_INTERVAL_MS, Platform.ALL, (c, s) -> c.cdcDirPollIntervalMs = Long.parseLong(s), c -> c.cdcDirPollIntervalMs,
                    "The poll interval in milliseconds for watching new commit log files in the CDC raw directory.",
                    60000L, "Long",
                    "main", 3);

    public static final String ERROR_COMMITLOG_REPROCESS_ENABLED = "errorCommitLogReprocessEnabled";
    public boolean errorCommitLogReprocessEnabled = Boolean.getBoolean(CDC_PROPERTY_PREFIX + ERROR_COMMITLOG_REPROCESS_ENABLED);
    public static final Setting<Boolean> ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING =
            new Setting<>(ERROR_COMMITLOG_REPROCESS_ENABLED, Platform.ALL, (c, s) -> c.errorCommitLogReprocessEnabled = Boolean.parseBoolean(s), c -> c.errorCommitLogReprocessEnabled,
                    "Enable the re-processing of error commit logs files.",
                    false, "Boolean",
                    "main", 4);

    public static final String CDC_CONCURRENT_PROCESSORS = "cdcConcurrentProcessors";
    public int cdcConcurrentProcessors = Integer.getInteger(CDC_PROPERTY_PREFIX + CDC_CONCURRENT_PROCESSORS, -1);
    public static final Setting<Integer> CDC_CONCURRENT_PROCESSOR_SETTING =
            new Setting<>(CDC_CONCURRENT_PROCESSORS, Platform.ALL, (c, s) -> c.cdcConcurrentProcessors = Integer.parseInt(s), c -> c.cdcConcurrentProcessors,
                    "The number of threads used to process commitlog files. The default value is the memtable_flush_writers.",
                    -1, "Integer",
                    "main", 5);

    public static final String MAX_INFLIGHT_MESSAGES_PER_TASK = "maxInflightMessagesPerTask";
    public int maxInflightMessagesPerTask = Integer.getInteger(CDC_PROPERTY_PREFIX + MAX_INFLIGHT_MESSAGES_PER_TASK, 16384);
    public static final Setting<Integer> MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING =
            new Setting<>(MAX_INFLIGHT_MESSAGES_PER_TASK, Platform.PULSAR, (c, s) -> c.maxInflightMessagesPerTask = Integer.parseInt(s), c -> c.maxInflightMessagesPerTask,
                    "The maximum number of in-flight messages per commitlog processing task.",
                    16384, "Integer",
                    "main", 6);

    public static final String SSL_PROVIDER = "sslProvider";
    public String sslProvider = System.getProperty(CDC_PROPERTY_PREFIX + SSL_PROVIDER);
    public static final Setting<String> SSL_PROVIDER_SETTING =
            new Setting<>(SSL_PROVIDER, Platform.ALL, (c, s) -> c.sslProvider = s, c -> c.sslProvider,
                    "The SSL/TLS provider to use.",
                    null, "String",
                    "ssl", 1);

    public static final String SSL_TRUSTSTORE_PATH = "sslTruststorePath";
    public String sslTruststorePath = System.getProperty(CDC_PROPERTY_PREFIX + SSL_TRUSTSTORE_PATH);
    public static final Setting<String> SSL_TRUSTSTORE_PATH_SETTING =
            new Setting<>(SSL_TRUSTSTORE_PATH, Platform.ALL, (c, s) -> c.sslTruststorePath = s, c -> c.sslTruststorePath,
                    "The path to the SSL/TLS truststore file.",
                    null, "String",
                    "ssl", 2);

    public static final String SSL_TRUSTSTORE_PASSWORD = "sslTruststorePassword";
    public String sslTruststorePassword = System.getProperty(CDC_PROPERTY_PREFIX + SSL_TRUSTSTORE_PASSWORD);
    public static final Setting<String> SSL_TRUSTSTORE_PASSWORD_SETTING =
            new Setting<>(SSL_TRUSTSTORE_PASSWORD, Platform.ALL, (c, s) -> c.sslTruststorePassword = s, c -> c.sslTruststorePassword,
                    "The password for the SSL/TLS truststore.",
                    null, "String",
                    "ssl", 3);

    public static final String SSL_TRUSTSTORE_TYPE = "sslTruststoreType";
    public String sslTruststoreType = System.getProperty(CDC_PROPERTY_PREFIX + SSL_TRUSTSTORE_TYPE, "JKS");
    public static final Setting<String> SSL_TRUSTSTORE_TYPE_SETTING =
            new Setting<>(SSL_TRUSTSTORE_TYPE, Platform.ALL, (c, s) -> c.sslTruststoreType = s, c -> c.sslTruststoreType,
                    "The type of the SSL/TLS truststore.",
                    "JKS", "String",
                    "ssl", 4);

    public static final String SSL_KEYSTORE_PATH = "sslKeystorePath";
    public String sslKeystorePath = System.getProperty(CDC_PROPERTY_PREFIX + SSL_KEYSTORE_PATH);
    public static final Setting<String> SSL_KEYSTORE_PATH_SETTING =
            new Setting<>(SSL_KEYSTORE_PATH, Platform.ALL, (c, s) -> c.sslKeystorePath = s, c -> c.sslKeystorePath,
                    "The path to the SSL/TLS keystore file.",
                    null, "String",
                    "ssl", 5);

    public static final String SSL_KEYSTORE_PASSWORD = "sslKeystorePassword";
    public String sslKeystorePassword = System.getProperty(CDC_PROPERTY_PREFIX + SSL_KEYSTORE_PASSWORD);
    public static final Setting<String> SSL_KEYSTORE_PASSWORD_SETTING =
            new Setting<>(SSL_KEYSTORE_PASSWORD, Platform.ALL, (c, s) -> c.sslKeystorePassword = s, c -> c.sslKeystorePassword,
                    "The password for the SSL/TLS keystore.",
                    null, "String",
                    "ssl", 6);

    public static final String SSL_CIPHER_SUITES = "sslCipherSuites";
    public String sslCipherSuites = System.getProperty(CDC_PROPERTY_PREFIX + SSL_CIPHER_SUITES);
    public static final Setting<String> SSL_CIPHER_SUITES_SETTING =
            new Setting<>(SSL_CIPHER_SUITES, Platform.ALL, (c, s) -> c.sslCipherSuites = s, c -> c.sslCipherSuites,
                    "Defines one or more cipher suites to use for negotiating the SSL/TLS connection.",
                    null, "String",
                    "ssl", 7);

    public static final String SSL_ENABLED_PROTOCOLS = "sslEnabledProtocols";
    public String sslEnabledProtocols = System.getProperty(CDC_PROPERTY_PREFIX + SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1,TLSv1");
    public static final Setting<String> SSL_ENABLED_PROTOCOLS_SETTING =
            new Setting<>(SSL_ENABLED_PROTOCOLS, Platform.ALL, (c, s) -> c.sslEnabledProtocols = s, c -> c.sslEnabledProtocols,
                    "Enabled SSL/TLS protocols",
                    "TLSv1.2,TLSv1.1,TLSv1", "String",
                    "ssl", 8);

    public static final String SSL_ALLOW_INSECURE_CONNECTION = "sslAllowInsecureConnection";
    public boolean sslAllowInsecureConnection = Boolean.getBoolean(CDC_PROPERTY_PREFIX + SSL_ALLOW_INSECURE_CONNECTION);
    public static final Setting<Boolean> SSL_ALLOW_INSECURE_CONNECTION_SETTING =
            new Setting<>(SSL_ALLOW_INSECURE_CONNECTION, Platform.PULSAR, (c, s) -> c.sslAllowInsecureConnection = Boolean.parseBoolean(s), c -> c.sslAllowInsecureConnection,
                    "Allows insecure connections to servers whose cert has not been signed by an approved CA. You should always disable sslAllowInsecureConnection in production environments.",
                    false, "Boolean",
                    "ssl", 10);

    public static final String SSL_HOSTNAME_VERIFICATION_ENABLE = "sslHostnameVerificationEnable";
    public boolean sslHostnameVerificationEnable = Boolean.getBoolean(CDC_PROPERTY_PREFIX + SSL_HOSTNAME_VERIFICATION_ENABLE);
    public static final Setting<Boolean> SSL_HOSTNAME_VERIFICATION_ENABLE_SETTING =
            new Setting<>(SSL_HOSTNAME_VERIFICATION_ENABLE, Platform.PULSAR, (c, s) -> c.sslHostnameVerificationEnable = Boolean.parseBoolean(s), c -> c.sslHostnameVerificationEnable,
                    "Enable the server hostname verification.",
                    false, "Boolean",
                    "ssl", 11);

    public static final String PULSAR_SERVICE_URL = "pulsarServiceUrl";
    public String pulsarServiceUrl = System.getProperty(CDC_PROPERTY_PREFIX + PULSAR_SERVICE_URL, "pulsar://localhost:6650");
    public static final Setting<String> PULSAR_SERVICE_URL_SETTING =
            new Setting<>(PULSAR_SERVICE_URL, Platform.PULSAR, (c, s) -> c.pulsarServiceUrl = s, c -> c.pulsarServiceUrl,
                    "The Pulsar broker service URL.",
                    "pulsar://localhost:6650", "String",
                    "pulsar", 1);

    public static final String PULSAR_BATCH_DELAY_IN_MS = "pulsarBatchDelayInMs";
    public long pulsarBatchDelayInMs = Long.getLong(CDC_PROPERTY_PREFIX + PULSAR_BATCH_DELAY_IN_MS, -1L);
    public static final Setting<Long> PULSAR_BATCH_BATCH_DELAY_IN_MS_SETTING =
            new Setting<>(PULSAR_BATCH_DELAY_IN_MS, Platform.PULSAR, (c, s) -> c.pulsarBatchDelayInMs = Long.parseLong(s), c -> c.pulsarBatchDelayInMs,
                    "Pulsar batching delay in milliseconds. Pulsar batching is enabled when this value is greater than zero.",
                    -1L, "Long",
                    "pulsar", 2);

    public static final String PULSAR_KEY_BASED_BATCHER = "pulsarKeyBasedBatcher";
    public boolean pulsarKeyBasedBatcher = Boolean.getBoolean(CDC_PROPERTY_PREFIX + PULSAR_KEY_BASED_BATCHER);
    public static final Setting<Boolean> PULSAR_KEY_BASED_BATCHER_SETTING =
            new Setting<>(PULSAR_KEY_BASED_BATCHER, Platform.PULSAR, (c, s) -> c.pulsarKeyBasedBatcher = Boolean.parseBoolean(s), c -> c.pulsarKeyBasedBatcher,
                    "When true, use the Pulsar KEY_BASED BatchBuilder",
                    false, "Boolean",
                    "pulsar", 3);

    public static final String PULSAR_MAX_PENDING_MESSAGES = "pulsarMaxPendingMessages";
    public int pulsarMaxPendingMessages = Integer.getInteger(CDC_PROPERTY_PREFIX + PULSAR_MAX_PENDING_MESSAGES, 1000);
    public static final Setting<Integer> PULSAR_MAX_PENDING_MESSAGES_SETTING =
            new Setting<>(PULSAR_MAX_PENDING_MESSAGES, Platform.PULSAR, (c, s) -> c.pulsarMaxPendingMessages = Integer.parseInt(s), c -> c.pulsarMaxPendingMessages,
                    "The Pulsar maximum size of a queue holding pending messages",
                    1000, "Integer",
                    "pulsar", 4);

    public static final String PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS= "pulsarMaxPendingMessagesAcrossPartitions";
    public int pulsarMaxPendingMessagesAcrossPartitions = Integer.getInteger(CDC_PROPERTY_PREFIX + PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS, 50000);
    public static final Setting<Integer> PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS_SETTING =
            new Setting<>(PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS, Platform.PULSAR, (c, s) -> c.pulsarMaxPendingMessagesAcrossPartitions = Integer.parseInt(s), c -> c.pulsarMaxPendingMessagesAcrossPartitions,
                    "The Pulsar maximum number of pending messages across partitions.",
                    50000, "Integer",
                    "pulsar", 5);

    public static final String PULSAR_AUTH_PLUGIN_CLASS_NAME = "pulsarAuthPluginClassName";
    public String pulsarAuthPluginClassName = System.getProperty(CDC_PROPERTY_PREFIX + PULSAR_AUTH_PLUGIN_CLASS_NAME);
    public static final Setting<String> PULSAR_AUTH_PLUGIN_CLASS_NAME_SETTING =
            new Setting<>(PULSAR_AUTH_PLUGIN_CLASS_NAME, Platform.PULSAR, (c, s) -> c.pulsarAuthPluginClassName = s, c -> c.pulsarAuthPluginClassName,
                    "The Pulsar authentication plugin class name.",
                    null, "String",
                    "pulsar", 6);

    public static final String PULSAR_AUTH_PARAMS = "pulsarAuthParams";
    public String pulsarAuthParams = System.getProperty(CDC_PROPERTY_PREFIX + PULSAR_AUTH_PARAMS);
    public static final Setting<String> PULSAR_AUTH_PARAMS_SETTING =
            new Setting<>(PULSAR_AUTH_PARAMS, Platform.PULSAR, (c, s) -> c.pulsarAuthParams = s, c -> c.pulsarAuthParams,
                    "The Pulsar authentication parameters.",
                    null, "String",
                    "pulsar", 7);

    public static final Set<Setting<?>> settings;
    public static final Map<String, Setting<?>> settingMap;

    static {
        // don't use guava
        Set<Setting<?>> set = new HashSet<>();
        set.add(CDC_RELOCATION_DIR_SETTING);
        set.add(CDC_DIR_POLL_INTERVAL_MS_SETTING);
        set.add(CDC_CONCURRENT_PROCESSOR_SETTING);
        set.add(ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING);
        set.add(TOPIC_PREFIX_SETTING);
        set.add(MAX_INFLIGHT_MESSAGES_PER_TASK_SETTING);
        set.add(SSL_PROVIDER_SETTING);
        set.add(SSL_TRUSTSTORE_PATH_SETTING);
        set.add(SSL_TRUSTSTORE_PASSWORD_SETTING);
        set.add(SSL_TRUSTSTORE_TYPE_SETTING);
        set.add(SSL_KEYSTORE_PATH_SETTING);
        set.add(SSL_KEYSTORE_PASSWORD_SETTING);
        set.add(SSL_CIPHER_SUITES_SETTING);
        set.add(SSL_ENABLED_PROTOCOLS_SETTING);
        set.add(SSL_ALLOW_INSECURE_CONNECTION_SETTING);
        set.add(SSL_HOSTNAME_VERIFICATION_ENABLE_SETTING);
        set.add(PULSAR_SERVICE_URL_SETTING);
        set.add(PULSAR_BATCH_BATCH_DELAY_IN_MS_SETTING);
        set.add(PULSAR_KEY_BASED_BATCHER_SETTING);
        set.add(PULSAR_MAX_PENDING_MESSAGES_SETTING);
        set.add(PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS_SETTING);
        set.add(PULSAR_AUTH_PLUGIN_CLASS_NAME_SETTING);
        set.add(PULSAR_AUTH_PARAMS_SETTING);
        settings = Collections.unmodifiableSet(set);

        Map<String, Setting<?>> map = new HashMap<>();
        settings.forEach(s -> map.put(s.name, s));
        settingMap = Collections.unmodifiableMap(map);
    }

    public static void main(String[] args) {
        try {
            String targetDir = args.length == 1 ? args[0] : "docs/modules/ROOT/pages";
            System.out.println("Generating agent parameter documentation in " + targetDir);
            generateAsciiDoc(Paths.get(targetDir), "agentParams.adoc", "Change Agent Parameters");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void generateAsciiDoc(Path path, String name, String title) throws IOException {
        // build ordered settings (ordered by group)
        List<String> groups = new LinkedList<>();
        List<Setting<?>> orderedSettings = new ArrayList<>(settings.size());
        settings.forEach(s -> {
            if (!groups.contains(s.group))
                groups.add(s.group);
            orderedSettings.add(s);
        });
        Collections.sort(groups);

        final Map<String, Integer> groupOrd = new HashMap<>();
        int ord = 0;
        for (String group : groups) {
            groupOrd.put(group, ord++);
        }
        Collections.sort(orderedSettings, (k1, k2) -> compare(k1, k2, groupOrd));

        try (FileWriter fileWriter = new FileWriter(path.resolve(name).toFile())) {
            PrintWriter pw = new PrintWriter(fileWriter);
            pw.append(".Table ").append(title).append("\n")
                    .append("[cols=\"2,3,1,1\"]\n")
                    .append("|===\n")
                    .append("|Name | Description | Type | Default\n");

            StringBuilder b = new StringBuilder();
            for (Setting<?> setting: orderedSettings) {
                setting.getAsciiDoc(b);
                b.append("\n");
            }

            pw.append(b.toString());
            pw.append("|===\n");
            pw.flush();
        }
    }

    protected static int compare(Setting<?> k1, Setting<?> k2, Map<String, Integer> groupOrd) {
        int cmp =
                k1.group == null
                        ? (k2.group == null ? 0 : -1)
                        : (k2.group == null
                        ? 1
                        : Integer.compare(groupOrd.get(k1.group), groupOrd.get(k2.group)));
        if (cmp == 0) {
            cmp = Integer.compare(k1.orderInGroup, k2.orderInGroup);
            if (cmp == 0) {
                // first take anything with no default value
                if (k1.defaultValue != null && k2.defaultValue == null) cmp = -1;
                else if (k2.defaultValue != null && k1.defaultValue == null) cmp = 1;
                else {
                    return k1.name.compareTo(k2.name);
                }
            }
        }
        return cmp;
    }

    public static AgentConfig create(Platform platform, Map<String, Object> tenantConfiguration) {
        return new AgentConfig().configure(platform, tenantConfiguration);
    }

    public static AgentConfig create(Platform platform, String agentParams) {
        return new AgentConfig().configure(platform, agentParams);
    }

    /**
     * Override the system properties with agent parameters.
     *
     * @param agentParameters
     */
    public AgentConfig configure(Platform platform, String agentParameters) {
        Map<String, Object> parameters = new HashMap<>();
        if (agentParameters != null) {
            for (String token : agentParameters.split("(?<!\\\\),\\s*")) {
                String param = token.replace("\\,", ",");
                int i = param.indexOf("=");
                if (i > 0) {
                    String key = param.substring(0, i);
                    String value = param.substring(i + 1);
                    parameters.put(key, value);
                }
            }
        }
        return configure(platform, parameters);
    }

    /**
     * Override the system properties with agent parameters.
     *
     * @param agentParameters
     */
    public AgentConfig configure(Platform platform, Map<String, Object> agentParameters) {
        if (agentParameters == null) {
            agentParameters = new HashMap<>();
        }
        for (Map.Entry<String, Object> entry : agentParameters.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() == null) {
                continue;
            }
            if (! (entry.getValue() instanceof String)) {
                throw new IllegalArgumentException(String.format("Unsupported parameter '%s' of type '%s', only String values are allowed ", key, entry.getValue().getClass()));
            }
            String value = (String) entry.getValue();
            Setting<?> setting = settingMap.get(key);
            if (setting != null) {
                if (!setting.platform.equals(Platform.ALL) && !setting.platform.equals(platform)) {
                    throw new IllegalArgumentException(String.format("Unsupported parameter '%s' for the %s platform ", key, platform));
                }
                setting.initializer.apply(this, value);
            } else {
                throw new RuntimeException(String.format("Unknown parameter '%s'", key));
            }
        }

        if (log.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            settings.forEach(s -> {
                if (s.platform.equals(Platform.ALL) || s.platform.equals(platform)) {
                    if (sb.length() > 0)
                        sb.append(", ");
                    sb.append(s.name).append("=").append(s.supplier.apply(this));
                }
            });
            log.info(sb.toString());
        }
        return this;
    }
}
