package com.datastax.oss.cdc.backfill.factory;

import com.datastax.oss.cdc.backfill.util.ConnectorUtils;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.typesafe.config.Config;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

/**
 * Factory for creating a {@link Connector} for reading from a DSBulk exported file
 */
public class ConnectorFactory {

    private final Path tableDataDir;

    public ConnectorFactory(Path tableDataDir) {
        this.tableDataDir = tableDataDir;
    }

    /**
     * Creates a new CSV connector for reading from a DSBulk exported file. Please note that the connector init() method
     * must be called after the export is done to determine exactly what files and folders need to be read.
     */
    public Connector newCVSConnector() throws URISyntaxException, IOException {
        CSVConnector connector = new CSVConnector();
        Config connectorConfig =
                ConnectorUtils.createConfig(
                        "dsbulk.connector.csv",
                        "url",
                        tableDataDir,
                        "recursive",
                        true,
                        "fileNamePattern",
                        "\"**/output-*\"");
        connector.configure(connectorConfig, true, true);
        connector.init();
        return connector;
    }
}
