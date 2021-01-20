/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.producer.exceptions.CassandraConnectorConfigException;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.*;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;


@Singleton
public class FileOffsetWriter implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileOffsetWriter.class);

    public static final String COMMITLOG_OFFSET_FILE = "commitlog_offset.dat";

    private final File offsetFile;
    private final CassandraConnectorConfiguration config;
    private AtomicReference<CommitLogPosition> offsetPositionRef = new AtomicReference<>(new CommitLogPosition(0,0));

    public FileOffsetWriter(CassandraConnectorConfiguration config) throws IOException {
        if (config.offsetBackingStoreDir == null) {
            throw new CassandraConnectorConfigException("Offset file directory must be configured at the start");
        }
        this.config = config;

        this.offsetFile = new File(config.offsetBackingStoreDir, COMMITLOG_OFFSET_FILE);
        init();
    }

    public CommitLogPosition position() {
        return this.offsetPositionRef.get();
    }

    public void markOffset(String sourceTable, CommitLogPosition sourceOffset) {
        this.offsetPositionRef.set(sourceOffset);
    }

    public void flush() throws IOException {
        saveOffset();
    }

    @Override
    public void close() throws IOException {
        saveOffset();
    }

    public static String serializePosition(CommitLogPosition commitLogPosition) {
        return Long.toString(commitLogPosition.segmentId) + File.pathSeparatorChar + Integer.toString(commitLogPosition.position);
    }

    public static CommitLogPosition deserializePosition(String s) {
        String[] segAndPos = s.split(Character.toString(File.pathSeparatorChar));
        return new CommitLogPosition(Long.parseLong(segAndPos[0]), Integer.parseInt(segAndPos[1]));
    }

    private synchronized void saveOffset() throws IOException {
        try(FileWriter out = new FileWriter(this.offsetFile)) {
            out.write(serializePosition(offsetPositionRef.get()));
        } catch (IOException e) {
            LOGGER.error("Failed to save offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    private synchronized void loadOffset() throws IOException {
        try(BufferedReader br = new BufferedReader(new FileReader(offsetFile)))
        {
            offsetPositionRef.set(deserializePosition(br.readLine()));
        } catch (IOException e) {
            LOGGER.error("Failed to load offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    private synchronized void init() throws IOException {
        if (offsetFile.exists()) {
            loadOffset();
        } else {
            Files.createDirectories( new File(config.offsetBackingStoreDir).toPath());
            saveOffset();
        }
    }
}
