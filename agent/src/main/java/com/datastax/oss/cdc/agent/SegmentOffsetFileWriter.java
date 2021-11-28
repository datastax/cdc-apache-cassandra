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

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

@Slf4j
public class SegmentOffsetFileWriter implements SegmentOffsetWriter, AutoCloseable {
    public static final String COMMITLOG_OFFSET_FILE_SUFFIX = "_offset.dat";
    public static final Pattern COMMITLOG_OFFSETS_REGEX_PATTERN = Pattern.compile("(\\d+)(_offset\\.dat)");

    private ConcurrentMap<Long, Integer> segmentOffsets = new ConcurrentHashMap<>();

    private final String cdcLogDir;

    public SegmentOffsetFileWriter(String cdcLogDir) throws IOException {
        this.cdcLogDir = cdcLogDir;
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

    public void position(Optional<UUID> nodeId, long segment, int position) {
        this.segmentOffsets.put(segment, position);
    }

    @Override
    public void flush(Optional<UUID> nodeId, long segmentId) throws IOException {
        saveOffset(segmentId);
    }

    @Override
    public void close() throws IOException {
        for(long seg : segmentOffsets.keySet())
            saveOffset(seg);
    }

    public static String serializePosition(int position) {
        return position + "";
    }

    public static int deserializePosition(String s) {
        return Integer.parseInt(s);
    }

    private void saveOffset(long segment) throws IOException {
        File segmentOffsetFile = new File(cdcLogDir, segment + COMMITLOG_OFFSET_FILE_SUFFIX);
        try(FileWriter out = new FileWriter(segmentOffsetFile)) {
            out.write(serializePosition(segmentOffsets.get(segment)));
        } catch (IOException e) {
            log.error("Failed to save offset for file " + segmentOffsetFile.getName(), e);
            throw e;
        }
    }

    public void loadOffset(long segment) throws IOException {
        File segmentOffsetFile = new File(cdcLogDir, segment + COMMITLOG_OFFSET_FILE_SUFFIX);
        if (segmentOffsetFile.exists()) {
            loadOffsetFile(segment, segmentOffsetFile);
        }
    }

    private void loadOffsetFile(long segment, File segmentOffsetFile) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(segmentOffsetFile))) {
            int position = deserializePosition(br.readLine());
            segmentOffsets.put(segment, position);
        } catch (IOException e) {
            log.error("Failed to load offset for file " + segmentOffsetFile.getName(), e);
            throw e;
        }
    }

    @Override
    public void loadOffsets() throws IOException {
        for(File f : new File(cdcLogDir).listFiles(f -> f.isFile() && COMMITLOG_OFFSETS_REGEX_PATTERN.matcher(f.getName()).matches())) {
            long segment = Long.parseLong(f.getName().substring(0, f.getName().length() - COMMITLOG_OFFSET_FILE_SUFFIX.length()));
            loadOffsetFile(segment, f);
        }
    }

    @Override
    public void remove(Optional<UUID> nodeId, long segment) {
        segmentOffsets.remove(segment);
        File segmentOffsetFile = new File(cdcLogDir, segment + COMMITLOG_OFFSET_FILE_SUFFIX);
        if (segmentOffsetFile.exists()) {
            segmentOffsetFile.delete();
        }
    }

    @Override
    public void remove(Optional<UUID> nodeId) {
        segmentOffsets.clear();
        for(File f : new File(cdcLogDir).listFiles(f -> f.isFile() && COMMITLOG_OFFSETS_REGEX_PATTERN.matcher(f.getName()).matches())) {
            f.delete();
        }
    }
}
