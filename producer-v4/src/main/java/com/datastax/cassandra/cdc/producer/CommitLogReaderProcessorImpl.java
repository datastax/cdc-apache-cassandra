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
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consume a queue of commitlog files to read mutations.
 *
 * @author vroyer
 */
@Slf4j
public class CommitLogReaderProcessorImpl extends CommitLogReaderProcessor implements AutoCloseable {

    private AtomicReference<CommitLogPosition> syncedOffsetRef = new AtomicReference<>(new CommitLogPosition(0,0));

    private CountDownLatch syncedOffsetLatch = new CountDownLatch(1);

    private final CommitLogReadHandlerImpl commitLogReadHandler;

    /**
     * The "workingCommitLog" is the most recent active commitlog file not yet completed.
     * We must avoid processing the workingCommitLog multiple times in parallel while it is updated.
     */
    final AtomicReference<File> workingCommitLog = new AtomicReference<>(null);
    final AtomicBoolean isProcessingWorkingCommitLog = new AtomicBoolean(false);

    /**
     * Consumes commitlog files in parallel.
     */
    ExecutorService processorExecutor;

    public CommitLogReaderProcessorImpl(ProducerConfig config,
                                        OffsetWriter offsetWriter,
                                        CommitLogTransfer commitLogTransfer,
                                        CommitLogReadHandlerImpl commitLogReadHandler) {
        super(config, offsetWriter, commitLogTransfer);
        this.commitLogReadHandler = commitLogReadHandler;
        this.processorExecutor = new JMXEnabledThreadPoolExecutor(
                1,
                DatabaseDescriptor.getConcurrentWriters(),
                DatabaseDescriptor.getCommitLogSyncPeriod() + 1000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("CdcCommitlogProcessor"),
                "CdcProducer");
    }

    public void submitCommitLog(File file) {
        log.debug("submitCommitLog file={}", file.getAbsolutePath());
        if (file.getName().endsWith("_cdc.idx")) {
            // you can have old _cdc.idx file, ignore it
            long seg = CommitLogUtil.extractTimestamp(file.getName());
            int pos = 0;
            if (seg >= this.syncedOffsetRef.get().segmentId) {
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
                        syncedOffsetRef.set(new CommitLogPosition(seg, pos));
                        String commitlogName = file.getName().substring(0, file.getName().length() - "_cdc.idx".length()) + ".log";
                        log.debug("New synced position={} completed={} adding file={}", syncedOffsetRef.get(), completed, commitlogName);
                        File commitlogFile = new File(file.getParentFile(), commitlogName);
                        if (completed) {
                            if (workingCommitLog.compareAndSet(commitlogFile, null)) {

                            }
                            this.processorExecutor.submit(new Task(commitlogFile, true));
                        } else {
                            this.workingCommitLog.set(commitlogFile);
                            if (isProcessingWorkingCommitLog.compareAndSet(false, true)) {
                                this.processorExecutor.submit(new Task(commitlogFile, false));
                            }
                        }

                        // unlock the processing of commitlogs
                        if(syncedOffsetLatch.getCount() > 0)
                            syncedOffsetLatch.countDown();
                    }
                } catch(IOException ex) {
                    log.warn("error while reading file=" + file.getName(), ex);
                }
            } else {
                log.debug("Ignoring old synced position from file={} pos={}", file.getName(), pos);
            }
        }
    }

    public void awaitSyncedPosition() throws InterruptedException {
        syncedOffsetLatch.await();
    }

    @Override
    public void start() throws Exception {
        // do nothing
    }

    @Override
    public void process() throws InterruptedException {
        // do nothing
    }

    @Override
    public void stop() throws Exception {
        processorExecutor.shutdown();
        processorExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * Process a commitlog file
     */
    public class Task implements Runnable {
        File file;
        boolean completed;

        public Task(File file, boolean completed) {
            this.file = file;
            this.completed = completed;
        }

        public void run() {
            if (!completed) {
                isProcessingWorkingCommitLog.set(true);
            }

            assert offsetWriter.offset().segmentId <= syncedOffsetRef.get().segmentId || offsetWriter.offset().position <= offsetWriter.offset().position : "file offset is greater than synced offset";

            if (!file.exists()) {
                log.debug("file={} does not exist any more, ignoring", file.getName());
                return;
            }
            long seg = CommitLogUtil.extractTimestamp(file.getName());

            // ignore file before the last write offset
            /*
            if (seg < offsetWriter.offset().segmentId) {
                log.debug("Ignoring file={} before the replicated segment={}", file.getName(), offsetWriter.offset().segmentId);
                return;
            }
             */

            // ignore file beyond the last synced commitlog, it will be re-queued on a file modification.
            if (seg > syncedOffsetRef.get().segmentId) {
                log.debug("Ignore a not synced file={}, last synced offset={}", file.getName(), syncedOffsetRef.get());
                return;
            }
            log.debug("processing file={} synced offset={}", file.getName(), syncedOffsetRef.get());
            assert seg <= syncedOffsetRef.get().segmentId : "reading a commitlog ahead the last synced offset";

            CommitLogReader commitLogReader = new CommitLogReader();
            try {
                // hack to use a dummy min position for segment ahead of the offsetFile.
                CommitLogPosition minPosition = (seg > offsetWriter.offset().segmentId)
                        ? new CommitLogPosition(seg, 0)
                        : new CommitLogPosition(offsetWriter.offset().getSegmentId(), offsetWriter.offset().getPosition());

                commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                log.debug("Successfully processed commitlog completed={} minPosition={} file={}",
                        seg < syncedOffsetRef.get().segmentId, minPosition, file.getName());

                if (completed) {
                    // do not transfer the active commitlog on Cassandra 4.x
                    commitLogTransfer.onSuccessTransfer(file.toPath());
                } else {
                    // flush sent offset
                    offsetWriter.flush();
                }
            } catch (Exception e) {
                log.warn("Failed to read commitlog completed=" + (seg < syncedOffsetRef.get().segmentId) + " file=" + file.getName(), e);
                if (completed) {
                    // do not transfer the active commitlog on Cassandra 4.x
                    commitLogTransfer.onErrorTransfer(file.toPath());
                }
            }

            if (!completed) {
                File workingCommitLogFile =  workingCommitLog.get();
                if (workingCommitLogFile != null) {
                    // submit a new task, the workingCommitlog has been updated.
                    processorExecutor.submit(new Task(workingCommitLogFile, false));
                }
                isProcessingWorkingCommitLog.set(false);
            }
        }
    }
}
