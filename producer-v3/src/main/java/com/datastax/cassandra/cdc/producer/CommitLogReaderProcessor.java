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
import java.util.concurrent.PriorityBlockingQueue;

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

    private final PriorityBlockingQueue<File> commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);
    private final CommitLogReadHandlerImpl commitLogReadHandler;
    private final OffsetFileWriter offsetFileWriter;
    private final CommitLogTransfer commitLogTransfer;
    private final ProducerConfig config;

    public CommitLogReaderProcessor(ProducerConfig config,
                                    CommitLogReadHandlerImpl commitLogReadHandler,
                                    OffsetFileWriter offsetFileWriter,
                                    CommitLogTransfer commitLogTransfer) {
        super(NAME, 0);
        this.config = config;
        this.commitLogReadHandler = commitLogReadHandler;
        this.offsetFileWriter = offsetFileWriter;
        this.commitLogTransfer = commitLogTransfer;
    }

    public void submitCommitLog(File file) {
        log.debug("submitCommitLog file={}", file.getAbsolutePath());
        this.commitLogQueue.add(file);
    }

    @Override
    public void process() throws InterruptedException {
        File file = null;
        while (true) {
            file = this.commitLogQueue.take();
            if (!file.exists()) {
                log.debug("file={} does not exist any more, ignoring", file.getName());
                continue;
            }
            long seg = CommitLogUtil.extractTimestamp(file.getName());

            // ignore file before the last write offset
            if (seg < this.offsetFileWriter.offset().segmentId) {
                log.debug("Ignoring file={} before the replicated segment={}", file.getName(), this.offsetFileWriter.offset().segmentId);
                continue;
            }

            log.debug("processing file={}", file.getName());

            CommitLogReader commitLogReader = new CommitLogReader();
            try {
                // hack to use a dummy min position for segment ahead of the offetFile.
                CommitLogPosition minPosition = (seg > offsetFileWriter.offset().segmentId)
                        ? new CommitLogPosition(seg, 0)
                        : new CommitLogPosition(offsetFileWriter.offset().getSegmentId(), offsetFileWriter.offset().getPosition());

                commitLogReader.readCommitLogSegment(commitLogReadHandler, file, false);
                log.debug("Successfully processed commitlog minPosition={} file={}", minPosition, file.getName());
                offsetFileWriter.flush(); // flush sent offset after each CL file
                commitLogTransfer.onSuccessTransfer(file.toPath());
            } catch (Exception e) {
                log.warn("Failed to read commitlog file=" + file.getName(), e);
                commitLogTransfer.onErrorTransfer(file.toPath());
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
