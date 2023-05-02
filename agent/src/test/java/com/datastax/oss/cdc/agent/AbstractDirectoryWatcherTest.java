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

import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AbstractDirectoryWatcherTest {

    private Path cdcDir;

    @BeforeEach
    public void init() throws IOException {
        cdcDir = Files.createTempDirectory("cdc");
    }

    @AfterEach
    void deleteTempDirs() {
        if (cdcDir != null && Files.exists(cdcDir)) {
            FileUtils.deleteDirectory(cdcDir);
        }
    }

    @Test
    public void testPoll() throws InterruptedException, IOException {
        // given
        Set<WatchEvent.Kind<?>> watchedEvents = Stream.of(ENTRY_CREATE, ENTRY_MODIFY, OVERFLOW).collect(Collectors.toSet());
        final BlockingQueue<File> commitLogQueue = new LinkedBlockingQueue<>();

        AbstractDirectoryWatcher watcher = new AbstractDirectoryWatcher(cdcDir,
                Duration.ofSeconds(20), // avoid flakiness, usually completes in ~10 seconds
                watchedEvents) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) {
                commitLogQueue.add(path.toFile());
            }
        };
        File commitLog = new File(cdcDir.toFile(), "CommitLog-6-1.log");
        commitLog.createNewFile();

        // when
        watcher.poll(); // can block up to 20 seconds

        // then
        assertEventHandled(commitLogQueue, commitLog);

        // write another file
        File commitLog2 = new File(cdcDir.toFile(), "CommitLog-6-2.log");
        commitLog2.createNewFile();

        // when
        watcher.poll(); // can block up to 20 seconds

        // then
        assertEventHandled(commitLogQueue, commitLog2);
    }

    private void assertEventHandled(BlockingQueue<File> commitLogQueue, File expectedFile)  {
        File actualFile = commitLogQueue.poll();
        assertNotNull(actualFile);
        assertEquals(commitLogQueue.size(), 0);
        assertEquals(expectedFile, actualFile);
    }
}
