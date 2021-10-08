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
package com.datastax.cassandra.cdc.producer;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class OffsetFileWriter implements OffsetWriter, AutoCloseable {
    public static final String COMMITLOG_OFFSET_FILE = "commitlog_offset.dat";

    private final File offsetFile;
    private AtomicReference<CommitLogPosition> fileOffsetRef = new AtomicReference<>(new CommitLogPosition(0,0));

    private final OffsetFlushPolicy offsetFlushPolicy;
    private volatile long timeOfLastFlush = System.currentTimeMillis();
    private volatile long notCommittedEvents = 0L;

    public OffsetFileWriter(String cdcWorkingDir) throws IOException {
        this.offsetFlushPolicy = new OffsetFlushPolicy.AlwaysFlushOffsetPolicy();
        this.offsetFile = new File(cdcWorkingDir, COMMITLOG_OFFSET_FILE);
        init();
    }

    @Override
    public CommitLogPosition offset(Optional<UUID> nodeId) {
        return this.fileOffsetRef.get();
    }

    @Override
    public void markOffset(Mutation<?> mutation) {
        this.fileOffsetRef.set(mutation.getCommitLogPosition());
        notCommittedEvents++;
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - timeOfLastFlush;
        if(offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), notCommittedEvents)) {
            flush(Optional.empty());
        }
    }

    @Override
    public void flush(Optional<UUID> nodeId) {
        saveOffset();
    }

    @Override
    public void close() {
        saveOffset();
    }

    public static String serializePosition(CommitLogPosition commitLogPosition) {
        return Long.toString(commitLogPosition.segmentId) + File.pathSeparatorChar + Integer.toString(commitLogPosition.position);
    }

    public static CommitLogPosition deserializePosition(String s) {
        String[] segAndPos = s.split(Character.toString(File.pathSeparatorChar));
        return new CommitLogPosition(Long.parseLong(segAndPos[0]), Integer.parseInt(segAndPos[1]));
    }

    private synchronized void saveOffset() {
        try(FileWriter out = new FileWriter(this.offsetFile)) {
            final CommitLogPosition position = fileOffsetRef.get();
            out.write(serializePosition(position));
            notCommittedEvents = 0L;
            timeOfLastFlush = System.currentTimeMillis();
            log.debug("Flush offset=" + position);
        } catch (IOException e) {
            log.error("Failed to save offset for file " + offsetFile.getName(), e);
        }
    }

    private synchronized void loadOffset() throws IOException {
        try(BufferedReader br = new BufferedReader(new FileReader(offsetFile))) {
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
            saveOffset();
        }
    }
}
