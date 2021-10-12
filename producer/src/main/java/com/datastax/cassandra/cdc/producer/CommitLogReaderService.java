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

@Slf4j
public abstract class CommitLogReaderService implements Runnable, AutoCloseable
{
    public static final String NAME = "CommitLogReader Processor";
    public static final String ARCHIVE_FOLDER = "archives";
    public static final String ERROR_FOLDER = "errors";

    /**
     * Submitted tasks to process CL files
     */
    static final ConcurrentMap<Long, Task> submittedTasks = new ConcurrentHashMap<>();

    /**
     * Pending tasks to process CL files
     */
    static final ConcurrentMap<Long, Task> pendingTasks = new ConcurrentHashMap<>();

    static int maxSubmittedTasks = 0;
    static int maxPendingTasks = 0;

    final ProducerConfig config;
    final SegmentOffsetWriter segmentOffsetWriter;
    final CommitLogTransfer commitLogTransfer;

    /**
     * ordered commitlog file queue.
     */
    final PriorityBlockingQueue<File> commitLogQueue;

    /**
     * Consumes commitlog files in parallel.
     */
    ExecutorService tasksExecutor;

    public CommitLogReaderService(ProducerConfig config,
                                  SegmentOffsetWriter segmentOffsetWriter,
                                  CommitLogTransfer commitLogTransfer) {
        this.config = config;
        this.segmentOffsetWriter = segmentOffsetWriter;
        this.commitLogTransfer = commitLogTransfer;
        this.commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);
    }

    @Override
    public void run() {
        while(true) {
            try {
                File file = commitLogQueue.take();
                submitCommitLog(file);
            } catch (InterruptedException e) {
                log.error("error:", e);
            }
        }
    }

    @Override
    public void close() {
        try {
            tasksExecutor.shutdown();
            tasksExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("error:", e);
        }
    }

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
                        if(lines.get(1).contains("COMPLETED")) {
                            completed = true;
                        }
                    } catch(Exception ex) {
                    }
                    Task runningTask = submittedTasks.get(seg);
                    if (pos > segmentOffsetWriter.position(seg) && (runningTask == null || pos > runningTask.syncPosition)) {
                        String commitlogName = file.getName().substring(0, file.getName().length() - "_cdc.idx".length()) + ".log";
                        Task task = createTask(commitlogName, seg, pos, completed);
                        pendingTasks.put(seg, task);
                        maxPendingTasks = Math.max(maxPendingTasks, pendingTasks.size());
                        log.debug("maxPendingTasks={}", maxPendingTasks);
                        tryToRunPendingTask(seg);
                    }
                }
            } catch(Exception ex) {
                log.warn("error while reading file=" + file.getName(), ex);
            }
        } else if (file.getName().endsWith(".log")) {
            try {
                long seg = CommitLogUtil.extractTimestamp(file.getName());
                Task task = createTask(file.getName(), seg, 0, true);
                pendingTasks.put(seg, task);
                maxPendingTasks = Math.max(maxPendingTasks, pendingTasks.size());
                log.debug("maxPendingTasks={}", maxPendingTasks);
                tryToRunPendingTask(seg);
            } catch(Exception ex) {
                log.warn("error while reading file=" + file.getName(), ex);
            }
        }
    }

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

    /**
     * Creates a task to process a commitlog file.
     * @param commitlogName
     * @param seg segment
     * @param pos last synced position
     * @param completed true when the commitlog file is COMPLETED (immutable)
     * @return
     */
    public abstract Task createTask(String commitlogName, long seg, int pos, boolean completed);


    /**
     * Start the pending task if not yet running.
     * This ensures that only on task per segment is running.
     * @param segment id
     * @return the running task or null
     */
    public Task tryToRunPendingTask(long segment) {
        return submittedTasks.compute(segment, (k1, v1) -> {
            if (v1 == null) {
                Task pendingTask = pendingTasks.get(segment);
                if (pendingTask != null) {
                    pendingTasks.remove(segment);
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


        public void finish(long segment) {
            submittedTasks.remove(segment);
            tryToRunPendingTask(segment);
        }
    }
}
