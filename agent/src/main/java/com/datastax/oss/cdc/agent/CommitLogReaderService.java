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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntBinaryOperator;
import java.util.function.LongBinaryOperator;

@Slf4j
public abstract class CommitLogReaderService implements Runnable, AutoCloseable
{
    public static final String ARCHIVE_FOLDER = "archives";
    public static final String ERROR_FOLDER = "errors";

    /**
     * Submitted tasks to process CL files
     */
    static final ConcurrentMap<Long, Task> submittedTasks = new ConcurrentHashMap<>();

    /**
     * Pending tasks to process CL files.
     */
    static final ConcurrentMap<Long, Task> pendingTasks = new ConcurrentHashMap<>();

    /**
     * Uncompleted segments task for delayed task cleanup
     */
    static final ConcurrentMap<Long, Task> uncleanedTasks = new ConcurrentHashMap<>();

    /**
     * Identify the working segment (not immutable) to properly garbageCollect immutable CL files.
     */
    static AtomicLong lastSegment = new AtomicLong(0);

    static AtomicInteger maxSubmittedTasks = new AtomicInteger(0);
    static AtomicInteger maxPendingTasks = new AtomicInteger(0);
    static AtomicInteger maxUncleanedTasks = new AtomicInteger(0);

    final AgentConfig config;
    final MutationSender<?> mutationSender;
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

    public CommitLogReaderService(AgentConfig config,
                                  MutationSender<?> mutationSender,
                                  SegmentOffsetWriter segmentOffsetWriter,
                                  CommitLogTransfer commitLogTransfer) {
        this.config = config;
        this.mutationSender = mutationSender;
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
        long seg = CommitLogUtil.extractTimestamp(file.getName());
        if (file.getName().endsWith("_cdc.idx")) {
            try {
                if (seg > lastSegment.get()) {
                    garbageCollect(seg);
                }
                lastSegment.getAndAccumulate(seg, Math::max);
                List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
                if (lines.size() > 0) {
                    int pos = Integer.parseInt(lines.get(0));
                    boolean completed = false;
                    try {
                        if(lines.get(1).contains("COMPLETED")) {
                            completed = true;
                        }
                    } catch(Exception ex) {
                    }
                    Task runningTask = submittedTasks.get(seg);
                    if (pos > segmentOffsetWriter.position(Optional.empty(), seg) && (runningTask == null || pos > runningTask.syncPosition)) {
                        String commitlogName = file.getName().substring(0, file.getName().length() - "_cdc.idx".length()) + ".log";
                        addPendingTask(createTask(commitlogName, seg, pos, completed));
                    }
                }
            } catch(Exception ex) {
                log.warn("error while reading file=" + file.getName(), ex);
            }
        } else if (file.getName().endsWith(".log")) {
            // Cassandra 3.x only path
            try {
                addPendingTask(createTask(file.getName(), seg, 0, true));
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


    public void addPendingTask(Task task) {
        pendingTasks.put(task.segment, task);
        maxPendingTasks.getAndAccumulate(pendingTasks.size(), Math::max);
        log.trace("maxPendingTasks={}", maxPendingTasks);
        maybeRunPendingTask(task.segment);
    }

    /**
     * Start the pending task if not yet running.
     * This ensures that only on task per segment is running.
     * @param segment id
     * @return the running task or null
     */
    public Task maybeRunPendingTask(long segment) {
        Task task = submittedTasks.compute(segment, (k1, v1) -> {
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
        maxSubmittedTasks.getAndAccumulate(submittedTasks.size(), Math::max);
        return task;
    }

    /**
     * Cleanup segments discarded by a Memtable flush but not marked COMPLETED.
     * This garbage collect immutable segments on disk and offsets from memory.
     * @param lastSegment the last segment id
     */
    private void garbageCollect(long lastSegment) {
        for(Map.Entry<Long, Task> entry : uncleanedTasks.entrySet()) {
            if (entry.getKey() < lastSegment
                    && submittedTasks.get(entry.getKey()) == null
                    && pendingTasks.get(entry.getKey()) == null) {
                entry.getValue().cleanup(entry.getValue().getStatus());
            }
        }
    }

    enum TaskStatus {
        SUCCESS,
        ERROR
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

        @EqualsAndHashCode.Exclude
        TaskStatus status = null;

        @EqualsAndHashCode.Exclude
        @ToString.Exclude
        volatile Throwable lastException = null;

        @EqualsAndHashCode.Exclude
        @ToString.Exclude
        Semaphore inflightMessagesSemaphore = new Semaphore(config.maxInflightMessagesPerTask);

        public Task(String filename, long segment, int syncPosition, boolean completed) {
            this.filename = filename;
            this.segment = segment;
            this.syncPosition = syncPosition;
            this.completed = completed;
        }

        public abstract File getFile();

        public void finish(TaskStatus taskStatus, int lastSentPosition) {
            if (taskStatus.equals(TaskStatus.SUCCESS)) {
                try {
                    log.debug("Task segment={} waiting for {} in-flight messages",
                            segment, config.maxInflightMessagesPerTask - inflightMessagesSemaphore.availablePermits());
                    inflightMessagesSemaphore.acquireUninterruptibly(config.maxInflightMessagesPerTask);
                    if (lastException != null)
                        throw lastException;
                    if (!completed && lastSentPosition > 0) {
                        // flush sent offset on disk to restart from that position
                        segmentOffsetWriter.position(Optional.empty(), segment, lastSentPosition);
                        segmentOffsetWriter.flush(Optional.empty(), segment);
                    }
                    log.debug("Task segment={} completed={} lastSentPosition={} succeed", segment, completed, lastSentPosition);
                } catch (Throwable e) {
                    // eventually resubmit self after 10s
                    log.error("Task segment={} completed={} syncPosition={} failed, retrying:", segment, completed, syncPosition, e);
                    pendingTasks.computeIfAbsent(segment, k -> {
                        // release all permits to avoid blocking retrying that task. Please note that the following code
                        // path will not be exercised there are other pending tasks for the same segment, in which case,
                        // that other pending would have its own, unused inflightMessagesSemaphore instance and there
                        // is no need to reset the semaphore on the current task.
                        log.debug("Task segment={} resubmitted, all inflightMessagesSemaphore permits will be released", segment);
                        inflightMessagesSemaphore.release(config.maxInflightMessagesPerTask -
                                inflightMessagesSemaphore.availablePermits());
                        return this;
                    });
                }
            }

            submittedTasks.remove(this.segment);
            Task nextTask = maybeRunPendingTask(this.segment);
            if (nextTask == null) {
                if (completed) {
                    uncleanedTasks.remove(segment);
                    cleanup(taskStatus);
                } else {
                    // task will be cleaned up when processing the next segment
                    this.status = taskStatus;
                    uncleanedTasks.put(segment, this);
                    maxUncleanedTasks.getAndAccumulate(uncleanedTasks.size(), Math::max);
                }
            }
        }

        public void cleanup(TaskStatus status) {
            log.debug("Cleanup task={}, status={}", this, status);
            File file = getFile();
            switch (status) {
                case SUCCESS:
                    commitLogTransfer.onSuccessTransfer(file.toPath());
                    break;
                case ERROR:
                    commitLogTransfer.onErrorTransfer(file.toPath());
                    break;
            }
            segmentOffsetWriter.remove(Optional.empty(), this.segment);
        }
    }
}
