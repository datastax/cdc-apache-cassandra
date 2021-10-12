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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

@Slf4j
public class SegmentOffsetDummyWriter implements SegmentOffsetWriter, AutoCloseable {

    public SegmentOffsetDummyWriter(String cdcLogDir) throws IOException {
    }

    @Override
    public int position(Optional<UUID> nodeId, long segmentId) {
        return 0;
    }

    public void position(Optional<UUID> nodeId, long segment, int position) {
    }

    @Override
    public void markOffset(Mutation<?> mutation) {
    }

    @Override
    public void flush(Optional<UUID> nodeId, long segmentId) throws IOException {
    }

    @Override
    public void remove(Optional<UUID> nodeId, long segmentId) {
    }

    @Override
    public void remove(Optional<UUID> nodeId) {
    }

    @Override
    public void close() throws IOException {
    }

}
