package com.datastax.cassandra.cdc.producer;

import java.io.File;

public class ProducerConfig {

    public static final String storageDir = System.getProperty("cassandra.storagedir", null);

    public static final String CDC_RELOCATION_DIR_SETTING = "cdcRelocationDir";
    public static String cdcRelocationDir = System.getProperty(CDC_RELOCATION_DIR_SETTING, storageDir + File.separator + "cdc_backup");

    public static final String CDC_DIR_POOL_INTERVAL_MS_SETTING = "cdcPoolIntervalMs";
    public static long cdcDirPollIntervalMs = Long.getLong(CDC_DIR_POOL_INTERVAL_MS_SETTING, 50000L);

    public static final String ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING = "errorCommitLogReprocessEnabled";
    public static boolean errorCommitLogReprocessEnabled = Boolean.getBoolean(ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING);

    public static final String EMIT_TOMBSTONE_ON_DELETE = "emitTombstoneOnDelete";
    public static boolean emitTombstoneOnDelete = Boolean.getBoolean(EMIT_TOMBSTONE_ON_DELETE);

    public static final String TOPIC_PREFIX_SETTING = "topicPrefix";
    public static String topicPrefix = System.getProperty(TOPIC_PREFIX_SETTING, "events-");

    public static final String PULSAR_SERVICE_URL_SETTING = "pulsarServiceUrl";
    public static String pulsarServiceUrl = System.getProperty(PULSAR_SERVICE_URL_SETTING, "pulsar://localhost:6650");

    public static final String KAFKA_BROKERS_SETTING = "kafakaBrokers";
    public static String kafkaBrokers = System.getProperty(KAFKA_BROKERS_SETTING, "localhost:9092");

    public static final String KAFKA_SCHEMA_REGISTRY_URL_SETTING = "kafkaSchemaRegistryUrl";
    public static String kafkaSchemaRegistryUrl = System.getProperty(KAFKA_SCHEMA_REGISTRY_URL_SETTING, "http://localhost:8081");

    /**
     * Override the system properties with agent parameters.
     *
     * @param agentParameters
     */
    public static void configure(String agentParameters) {
        for (String param : agentParameters.split(",")) {
            String[] kv = param.split("=");
            if (kv.length == 2) {
                String key = kv[0];
                String value = kv[1];

                if (TOPIC_PREFIX_SETTING.equals(key)) {
                    topicPrefix = value;
                } else if (PULSAR_SERVICE_URL_SETTING.equals(key)) {
                    pulsarServiceUrl = value;
                } else if (KAFKA_BROKERS_SETTING.equals(key)) {
                    kafkaBrokers = value;
                } else if (KAFKA_SCHEMA_REGISTRY_URL_SETTING.equals(key)) {
                    kafkaSchemaRegistryUrl = value;
                } else if (CDC_DIR_POOL_INTERVAL_MS_SETTING.equals(key)) {
                    cdcDirPollIntervalMs = Long.parseLong(value);
                } else if (ERROR_COMMITLOG_REPROCESS_ENABLED_SETTING.equals(key)) {
                    errorCommitLogReprocessEnabled = Boolean.parseBoolean(value);
                } else if (CDC_RELOCATION_DIR_SETTING.equals(key)) {
                    cdcRelocationDir = value;
                }
            }
        }
    }
}
