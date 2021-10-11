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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class SegmentOffsetFileWriter implements SegmentOffsetWriter, AutoCloseable {
    public static final String COMMITLOG_OFFSET_FILE_SUFFIX = "_offset.dat";

    private ConcurrentMap<Long, Integer> segmentOffsets = new ConcurrentHashMap<>();

    private final String cdcLogDir;
    private final OffsetFlushPolicy offsetFlushPolicy;
    private volatile long timeOfLastFlush = System.currentTimeMillis();
    private volatile long notCommittedEvents = 0L;

    public SegmentOffsetFileWriter(String cdcLogDir) throws IOException {
        this.cdcLogDir = cdcLogDir;
        this.offsetFlushPolicy = new OffsetFlushPolicy.AlwaysFlushOffsetPolicy();

        File cdcLogFile = new File(cdcLogDir);
        if (!cdcLogFile.exists())
            Files.createDirectories(cdcLogFile.toPath());
    }

    @Override
    public int position(Optional<UUID> nodeId, long segmentId) {
        return segmentOffsets.containsKey(segmentId)
                ? segmentOffsets.get(segmentId)
                : 0;
    }

    @Override
    public void markOffset(Mutation<?> mutation) {
        this.segmentOffsets.put(mutation.getCommitLogPosition().segmentId, mutation.getCommitLogPosition().position);
        notCommittedEvents++;
        long now = System.currentTimeMillis();
        long timeSinceLastFlush = now - timeOfLastFlush;
        if(offsetFlushPolicy.shouldFlush(Duration.ofMillis(timeSinceLastFlush), notCommittedEvents)) {
            flush(Optional.empty(), mutation.getCommitLogPosition().segmentId);
        }
    }

    @Override
    public void flush(Optional<UUID> nodeId, long segmentId) {
        saveOffset(segmentId);
    }

    @Override
    public void close() {
        segmentOffsets.keySet().forEach(seg -> saveOffset(seg));
    }

    public static String serializePosition(CommitLogPosition commitLogPosition) {
        return commitLogPosition.position + "";
    }

    public static int deserializePosition(String s) {
        return Integer.parseInt(s);
    }

    private synchronized void saveOffset(long segment) {
        File segmentOffsetFile = new File(cdcLogDir, segment + COMMITLOG_OFFSET_FILE_SUFFIX);
        try(FileWriter out = new FileWriter(segmentOffsetFile)) {
            final CommitLogPosition position = new CommitLogPosition(segment, segmentOffsets.get(segment));
            out.write(serializePosition(position));
            notCommittedEvents = 0L;
            timeOfLastFlush = System.currentTimeMillis();
            log.debug("Flush offset=" + position);
        } catch (IOException e) {
            log.error("Failed to save offset for file " + segmentOffsetFile.getName(), e);
        }
    }

    private synchronized void loadOffset(long segment) throws IOException {
        File segmentOffsetFile = new File(cdcLogDir, segment + COMMITLOG_OFFSET_FILE_SUFFIX);
        if (segmentOffsetFile.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(segmentOffsetFile))) {
                int position = deserializePosition(br.readLine());
                segmentOffsets.put(segment, position);
                log.debug("loaded segment={} position={}", segment, position);
            } catch (IOException e) {
                log.error("Failed to load offset for file " + segmentOffsetFile.getName(), e);
                throw e;
            }
        }
    }

    @Override
    public void remove(long segment) {
        segmentOffsets.remove(segment);
        File segmentOffsetFile = new File(cdcLogDir, segment + COMMITLOG_OFFSET_FILE_SUFFIX);
        if (segmentOffsetFile.exists()) {
            segmentOffsetFile.delete();
        }
    }

}
