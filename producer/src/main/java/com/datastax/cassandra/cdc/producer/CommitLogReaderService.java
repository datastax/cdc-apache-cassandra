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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class CommitLogReaderService
{
    public static final String NAME = "CommitLogReader Processor";
    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    static final ConcurrentMap<Long, Task> runningTasks = new ConcurrentHashMap<>();
    static final ConcurrentMap<Long, Task> pendingTasks = new ConcurrentHashMap<>();

    static int maxRunningTasksGauge = 0;
    static int maxPendingTasksGauge = 0;

    final ProducerConfig config;
    final SegmentOffsetWriter segmentOffsetWriter;
    final CommitLogTransfer commitLogTransfer;

    /**
     * COMPLETED commitlog file queue.
     */
    final PriorityBlockingQueue<File> commitLogQueue;

    public CommitLogReaderService(ProducerConfig config,
                                  SegmentOffsetWriter segmentOffsetWriter,
                                  CommitLogTransfer commitLogTransfer) {
        this.config = config;
        this.segmentOffsetWriter = segmentOffsetWriter;
        this.commitLogTransfer = commitLogTransfer;
        this.commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);
    }

    /**
     * Consumes commitlog files in parallel.
     */
    ExecutorService tasksExecutor;

    public void submitCommitLog(File file) {
        log.debug("submitCommitLog file={}", file.getAbsolutePath());
        if (file.getName().endsWith("_cdc.idx")) {
            // you can have old _cdc.idx file, ignore it
            long seg = CommitLogUtil.extractTimestamp(file.getName());
            int pos = 0;
            try {
                List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
                if (lines.size() > 0) {
                    pos = Integer.parseInt(lines.get(0));
                    boolean completed = false;
                    try {
                        if("COMPLETED".equals(lines.get(1))) {
                            completed = true;
                        }
                    } catch(Exception ex) {
                    }
                    String commitlogName = file.getName().substring(0, file.getName().length() - "_cdc.idx".length()) + ".log";
                    Task task = createTask(commitlogName, seg, pos, completed);
                    pendingTasks.put(seg, task);
                    maxPendingTasksGauge = Math.max(maxPendingTasksGauge, pendingTasks.size());
                    tryToRunPendingTask(seg);
                }
            } catch(IOException ex) {
                log.warn("error while reading file=" + file.getName(), ex);
            }
        }
    }

    public abstract Task createTask(String commitlogName, long seg, int pos, boolean completed);

    public void initialize() throws Exception {

        File relocationDir = new File(config.cdcWorkingDir);
        if (!relocationDir.exists()) {
            if (!relocationDir.mkdir()) {
                throw new IOException("Failed to create " + config.cdcWorkingDir);
            }
        }

        File archiveDir = new File(relocationDir, ARCHIVE_FOLDER);
        if (!archiveDir.exists()) {
            if (!archiveDir.mkdir()) {
                throw new IOException("Failed to create " + archiveDir);
            }
        }
        File errorDir = new File(relocationDir, ERROR_FOLDER);
        if (!errorDir.exists()) {
            if (!errorDir.mkdir()) {
                throw new IOException("Failed to create " + errorDir);
            }
        }
    }

    public void stop() throws Exception {
        tasksExecutor.shutdown();
        tasksExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * Start the pending task if not yet running.
     * @param segment id
     * @return the running task or null
     */
    public Task tryToRunPendingTask(long segment) {
        return runningTasks.compute(segment, (k1, v1) -> {
            if (v1 == null) {
                Task pendingTask = pendingTasks.get(segment);
                if (pendingTask != null) {
                    pendingTasks.remove(pendingTask);
                    tasksExecutor.submit(pendingTask);
                }
                return pendingTask;
            }
            return v1;
        });
    }

    /**
     * commitlog file task
     */
    @EqualsAndHashCode
    @ToString
    @Getter
    public abstract class Task implements Runnable {
        final String filename;
        final long segment;
        final int syncPosition;
        final boolean completed;

        public Task(String filename, long segment, int syncPosition, boolean completed) {
            this.filename = filename;
            this.segment = segment;
            this.syncPosition = syncPosition;
            this.completed = completed;
        }
    }
}
