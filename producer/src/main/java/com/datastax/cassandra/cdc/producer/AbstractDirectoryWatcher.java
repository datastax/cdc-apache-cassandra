/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.cdc.producer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper class around WatchService to make WatchService re-usable and avoid code repetition
 */
@Slf4j
public abstract class AbstractDirectoryWatcher {

    private final WatchService watchService;
    private final Duration pollInterval;
    private final Path directory;
    private final Set<WatchEvent.Kind<Path>> kinds;

    public AbstractDirectoryWatcher(Path directory, Duration pollInterval, Set<WatchEvent.Kind<Path>> kinds) throws IOException {
        this(FileSystems.getDefault().newWatchService(), directory, pollInterval, kinds);
    }

    AbstractDirectoryWatcher(WatchService watchService, Path directory, Duration pollInterval, Set<WatchEvent.Kind<Path>> kinds) throws IOException {
        this.watchService = watchService;
        this.pollInterval = pollInterval;
        this.directory = directory;
        this.kinds = kinds;

        directory.register(watchService, kinds.toArray(new WatchEvent.Kind<?>[kinds.size()]));
    }

    public void poll() throws InterruptedException, IOException {
        WatchKey key = watchService.poll(pollInterval.toMillis(), TimeUnit.MILLISECONDS);

        if (key != null) {
            for (WatchEvent<?> event : key.pollEvents()) {
                Path relativePath = (Path) event.context();
                Path absolutePath = directory.resolve(relativePath);

                if (kinds.contains(event.kind())) {
                    log.debug("Detected new commitlog file={}", absolutePath);
                    handleEvent(event, absolutePath);
                }
            }
            key.reset();
        }
    }

    abstract void handleEvent(WatchEvent<?> event, Path path) throws IOException;
}
