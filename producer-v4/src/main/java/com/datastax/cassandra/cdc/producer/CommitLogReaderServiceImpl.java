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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import javax.swing.text.html.Option;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Consume a queue of commitlog files to read mutations.
 *
 * @author vroyer
 */
@Slf4j
public class CommitLogReaderServiceImpl extends CommitLogReaderService {

    private final CommitLogReadHandlerImpl commitLogReadHandler;

    public CommitLogReaderServiceImpl(ProducerConfig config,
                                      SegmentOffsetWriter segmentOffsetWriter,
                                      CommitLogTransfer commitLogTransfer,
                                      CommitLogReadHandlerImpl commitLogReadHandler) {
        super(config, segmentOffsetWriter, commitLogTransfer);
        this.commitLogReadHandler = commitLogReadHandler;
        this.tasksExecutor = new JMXEnabledThreadPoolExecutor(
                1,
                config.cdcConcurrentProcessor == -1 ? DatabaseDescriptor.getFlushWriters() : config.cdcConcurrentProcessor,
                DatabaseDescriptor.getCommitLogSyncPeriod() + 1000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("CdcCommitlogProcessor"),
                "CdcProducer");
    }

    public Task createTask(String filename, long segment, int syncPosition, boolean completed) {
        return new Task(filename, segment, syncPosition, completed) {

            public void run() {
                maxSubmittedTasks = Math.max(maxSubmittedTasks, submittedTasks.size());
                log.debug("Starting task={}", this);
                File file = getFile();
                try {
                    if (!file.exists()) {
                        log.warn("file={} does not exist any more, ignoring", file.getName());
                        finish(TaskStatus.SUCCESS);
                        return;
                    }
                    long seg = CommitLogUtil.extractTimestamp(file.getName());
                    CommitLogReader commitLogReader = new CommitLogReader();
                    if (syncPosition > segmentOffsetWriter.position(Optional.empty(), seg)) {
                        CommitLogPosition minPosition = new CommitLogPosition(seg, segmentOffsetWriter.position(Optional.empty(), seg));
                        commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                        log.debug("Task executed {}", this);

                        if (!completed) {
                            // flush sent offset on disk to restart from that position
                            segmentOffsetWriter.flush(Optional.empty(), seg);
                        }
                    }
                    finish(TaskStatus.SUCCESS);
                } catch (Exception e) {
                    log.warn("Task failed {}", this, e);
                    finish(TaskStatus.ERROR);
                } finally {
                    CdcMetrics.executedTasks.inc();
                }
            }

            public File getFile() {
                return new File(DatabaseDescriptor.getCDCLogLocation(), filename);
            }
        };
    }
}
