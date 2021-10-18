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
import org.apache.cassandra.schema.TableMetadata;

import java.io.File;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.*;

/**
 * Consume a queue of commitlog files to read mutations.
 */
@Slf4j
public class CommitLogReaderServiceImpl extends CommitLogReaderService {


    public CommitLogReaderServiceImpl(ProducerConfig config,
                                      MutationSender<TableMetadata> mutationSender,
                                      SegmentOffsetWriter segmentOffsetWriter,
                                      CommitLogTransfer commitLogTransfer) {
        super(config, mutationSender, segmentOffsetWriter, commitLogTransfer);
        this.tasksExecutor = new JMXEnabledThreadPoolExecutor(
                1,
                config.cdcConcurrentProcessor == -1 ? DatabaseDescriptor.getFlushWriters() : config.cdcConcurrentProcessor,
                DatabaseDescriptor.getCommitLogSyncPeriod() + 1000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("CdcCommitlogProcessor"),
                "CdcProducer",
                new ThreadPoolExecutor.AbortPolicy()
                );
    }

    @SuppressWarnings("unchecked")
    public Task createTask(String filename, long segment, int syncPosition, boolean completed) {
        return new Task(filename, segment, syncPosition, completed) {

            public void run() {
                maxSubmittedTasks = Math.max(maxSubmittedTasks, submittedTasks.size());
                sentMutations = new Vector<>();
                log.debug("Starting task={}", this);
                File file = getFile();
                try {
                    int markedPosition = -1;
                    if (!file.exists()) {
                        log.warn("file={} does not exist any more, ignoring", file.getName());
                        finish(TaskStatus.SUCCESS, markedPosition);
                        return;
                    }
                    long seg = CommitLogUtil.extractTimestamp(file.getName());

                    if (syncPosition > segmentOffsetWriter.position(Optional.empty(), seg)) {
                        CommitLogPosition minPosition = new CommitLogPosition(seg, segmentOffsetWriter.position(Optional.empty(), seg));
                        CommitLogReadHandlerImpl commitLogReadHandler = new CommitLogReadHandlerImpl(config, (MutationSender<TableMetadata>) mutationSender, this);
                        CommitLogReader commitLogReader = new CommitLogReader();
                        commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                        markedPosition = commitLogReadHandler.getMarkedPosition();
                    }
                    finish(TaskStatus.SUCCESS, markedPosition);
                } catch (Exception e) {
                    log.warn("Task failed {}", this, e);
                    finish(TaskStatus.ERROR, -1);
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
