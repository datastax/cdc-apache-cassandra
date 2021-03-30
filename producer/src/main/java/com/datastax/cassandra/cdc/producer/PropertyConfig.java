package com.datastax.cassandra.cdc.producer;

import org.apache.cassandra.config.Config;

import java.io.File;

public class PropertyConfig {
    public static final String storageDir = System.getProperty(Config.PROPERTY_PREFIX + "storagedir", null);

    public static final String cdcRelocationDir = System.getProperty(Config.PROPERTY_PREFIX + "cdcRelocationDir",
            storageDir + File.separator + "cdc_backup");

    public static final Long cdcDirPollIntervalMs = Long.getLong(Config.PROPERTY_PREFIX + "cdcPoolIntervalMs", 10000L);

    public static final boolean errorCommitLogReprocessEnabled = Boolean.getBoolean(Config.PROPERTY_PREFIX + "errorCommitLogReprocessEnabled");

    public static final boolean emitTombstoneOnDelete = true;


    public static final String pulsarTopicPrefix = System.getProperty("pulsarTopicPrefix", "dirty-");
    public static final String pulsarServiceUrl = System.getProperty("pulsarServiceUrl", "pulsar://localhost:6650");
}
