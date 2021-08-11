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
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consume a queue of commitlog files to read mutations.
 *
 * @author vroyer
 */
@Slf4j
public class CommitLogReaderProcessor extends AbstractProcessor implements AutoCloseable {
    private static final String NAME = "CommitLogReader Processor";

    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    // synced position
    private AtomicReference<CommitLogPosition> syncedOffsetRef = new AtomicReference<>(new CommitLogPosition(0,0));

    private CountDownLatch syncedOffsetLatch = new CountDownLatch(1);

    private final PriorityBlockingQueue<File> commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);

    private final CommitLogReadHandlerImpl commitLogReadHandler;
    private final OffsetWriter offsetWriter;
    private final CommitLogTransfer commitLogTransfer;
    private final ProducerConfig config;

    public CommitLogReaderProcessor(ProducerConfig config,
                                    CommitLogReadHandlerImpl commitLogReadHandler,
                                    OffsetWriter offsetWriter,
                                    CommitLogTransfer commitLogTransfer) {
        super(NAME, 0);
        this.config = config;
        this.commitLogReadHandler = commitLogReadHandler;
        this.offsetWriter = offsetWriter;
        this.commitLogTransfer = commitLogTransfer;
    }

    public void submitCommitLog(File file)  {
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
                        String commitlogName = file.getName().substring(0, file.getName().length() - 8) + ".log";
                        log.debug("New synced position={} completed={} adding file={}", syncedOffsetRef.get(), completed, commitlogName);
                        this.commitLogQueue.add(new File(file.getParentFile(), commitlogName));

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
        } else {
            this.commitLogQueue.add(file);
        }
    }

    public void awaitSyncedPosition() throws InterruptedException {
        syncedOffsetLatch.await();
    }

    @Override
    public void process() throws InterruptedException {
        assert this.offsetWriter.offset().segmentId <= this.syncedOffsetRef.get().segmentId || this.offsetWriter.offset().position <= this.offsetWriter.offset().position : "file offset is greater than synced offset";
        File file = null;
        while(true) {
            file = this.commitLogQueue.take();

            if (!file.exists()) {
                log.debug("file={} does not exist any more, ignoring", file.getName());
                continue;
            }
            long seg = CommitLogUtil.extractTimestamp(file.getName());

            // ignore file before the last write offset
            if (seg < this.offsetWriter.offset().segmentId) {
                log.debug("Ignoring file={} before the replicated segment={}", file.getName(), this.offsetWriter.offset().segmentId);
                continue;
            }
            // ignore file beyond the last synced commitlog, it will be re-queued on a file modification.
            if (seg > this.syncedOffsetRef.get().segmentId) {
                log.debug("Ignore a not synced file={}, last synced offset={}", file.getName(), this.syncedOffsetRef.get());
                continue;
            }
            log.debug("processing file={} synced offset={}", file.getName(), this.syncedOffsetRef.get());
            assert seg <= this.syncedOffsetRef.get().segmentId: "reading a commitlog ahead the last synced offset";

            CommitLogReader commitLogReader = new CommitLogReader();
            try {
                // hack to use a dummy min position for segment ahead of the offsetFile.
                CommitLogPosition minPosition = (seg > offsetWriter.offset().segmentId)
                        ? new CommitLogPosition(seg, 0)
                        : new CommitLogPosition(offsetWriter.offset().getSegmentId(), offsetWriter.offset().getPosition());

                commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                log.debug("Successfully processed commitlog completed={} minPosition={} file={}",
                        seg < this.syncedOffsetRef.get().segmentId, minPosition, file.getName());
                offsetWriter.flush(); // flush sent offset after each CL file
                if (seg < this.syncedOffsetRef.get().segmentId) {
                    // do not transfer the active commitlog on Cassandra 4.x
                    commitLogTransfer.onSuccessTransfer(file.toPath());
                }
            } catch(Exception e) {
                log.warn("Failed to read commitlog completed="+(seg < this.syncedOffsetRef.get().segmentId)+" file="+file.getName(), e);
                if (seg < this.syncedOffsetRef.get().segmentId) {
                    // do not transfer the active commitlog on Cassandra 4.x
                    commitLogTransfer.onErrorTransfer(file.toPath());
                }
            }
        }
    }

    @Override
    public void initialize() throws Exception {

        File relocationDir = new File(config.cdcRelocationDir);

        if (!relocationDir.exists()) {
            if (!relocationDir.mkdir()) {
                throw new IOException("Failed to create " + config.cdcRelocationDir);
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
     * Override destroy to clean up resources after stopping the processor
     */
    @Override
    public void close() {
    }
}
