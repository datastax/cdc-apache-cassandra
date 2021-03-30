/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class OffsetFileWriter implements AutoCloseable {
    public static final String COMMITLOG_OFFSET_FILE = "commitlog_offset.dat";

    private final File offsetFile;
    private AtomicReference<CommitLogPosition> fileOffsetRef = new AtomicReference<>(new CommitLogPosition(0,0));

    private final OffsetFlushPolicy offsetFlushPolicy;
    volatile long timeOfLastFlush = System.currentTimeMillis();
    volatile Long notCommittedEvents = 0L;

    public OffsetFileWriter() throws IOException {
        this.offsetFlushPolicy = new OffsetFlushPolicy.AlwaysFlushOffsetPolicy();
        /*
        this.meterRegistry.gauge("committed_segment", fileOffsetRef, new ToDoubleFunction<AtomicReference<CommitLogPosition>>() {
            @Override
            public double applyAsDouble(AtomicReference<CommitLogPosition> offsetPositionRef) {
                return offsetPositionRef.get().segmentId;
            }
        });
        this.meterRegistry.gauge("committed_position", fileOffsetRef, new ToDoubleFunction<AtomicReference<CommitLogPosition>>() {
            @Override
            public double applyAsDouble(AtomicReference<CommitLogPosition> offsetPositionRef) {
                return offsetPositionRef.get().position;
            }
        });
         */

        this.offsetFile = new File(DatabaseDescriptor.getCDCLogLocation(), COMMITLOG_OFFSET_FILE);
        init();
    }

    public CommitLogPosition offset() {
        return this.fileOffsetRef.get();
    }

    public void markOffset(CommitLogPosition sourceOffset) {
        this.fileOffsetRef.set(sourceOffset);
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
            out.write(serializePosition(fileOffsetRef.get()));
        } catch (IOException e) {
            log.error("Failed to save offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    private synchronized void loadOffset() throws IOException {
        try(BufferedReader br = new BufferedReader(new FileReader(offsetFile)))
        {
            fileOffsetRef.set(deserializePosition(br.readLine()));
            log.debug("file offset={}", fileOffsetRef.get());
        } catch (IOException e) {
            log.error("Failed to load offset for file " + offsetFile.getName(), e);
            throw e;
        }
    }

    private synchronized void init() throws IOException {
        if (offsetFile.exists()) {
            loadOffset();
        } else {
            Path parentPath = offsetFile.toPath().getParent();
            if (!parentPath.toFile().exists())
                Files.createDirectories(parentPath);
            saveOffset();
        }
    }

    void maybeCommitOffset(Mutation record) {
        try {
            long now = System.currentTimeMillis();
            long timeSinceLastFlush = now - timeOfLastFlush;
            if(offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), notCommittedEvents)) {
                SourceInfo source = record.getSource();
                markOffset(source.commitLogPosition);
                flush();
                //this.meterRegistry.counter(MetricConstants.METRICS_PREFIX + "commit").increment();
                //this.meterRegistry.counter(MetricConstants.METRICS_PREFIX + "committed").increment(notCommittedEvents);
                notCommittedEvents = 0L;
                timeOfLastFlush = now;
                log.debug("Offset flushed source=" + source);
            }
        } catch(IOException e) {
            log.warn("error:", e);
        }
    }
}
