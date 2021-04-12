package com.datastax.cassandra.cdc.producer;

import java.io.File;

public class PropertyConfig {
    public static final String PROPERTY_PREFIX = "cassandra.";

    public static final String storageDir = System.getProperty(PROPERTY_PREFIX + "storagedir", null);

    public static final String cdcRelocationDir = System.getProperty(PROPERTY_PREFIX + "cdcRelocationDir",
            storageDir + File.separator + "cdc_backup");

    public static final Long cdcDirPollIntervalMs = Long.getLong(PROPERTY_PREFIX + "cdcPoolIntervalMs", 10000L);

    public static final boolean errorCommitLogReprocessEnabled = Boolean.getBoolean(PROPERTY_PREFIX + "errorCommitLogReprocessEnabled");

    public static final boolean emitTombstoneOnDelete = true;


    public static final String topicPrefix = System.getProperty("topicPrefix", "events-");

    public static final String pulsarServiceUrl = System.getProperty("pulsarServiceUrl", "pulsar://localhost:6650");

    public static final String kafkaBrokers = System.getProperty("kafkaBrokers", "localhost:9092");
    public static final String kafkaRegistryUrl = System.getProperty("schemaRegistryUrl", "http://localhost:8081");
}
