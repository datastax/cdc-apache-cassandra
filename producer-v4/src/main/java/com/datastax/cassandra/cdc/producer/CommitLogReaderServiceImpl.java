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
                config.cdcConcurrentProcessors == -1 ? DatabaseDescriptor.getFlushWriters() : config.cdcConcurrentProcessors,
                config.cdcConcurrentProcessors == -1 ? DatabaseDescriptor.getFlushWriters() : config.cdcConcurrentProcessors,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("CdcCommitlogProcessor"),
                CdcMetrics.CDC_PRODUCER_MBEAN_NAME);
    }

    @SuppressWarnings("unchecked")
    public Task createTask(String filename, long segment, int syncPosition, boolean completed) {
        return new Task(filename, segment, syncPosition, completed) {

            public void run() {
                maxSubmittedTasks = Math.max(maxSubmittedTasks, submittedTasks.size());
                pendingPositions = new ArrayBlockingQueue<>(config.pulsarMaxPendingMessagesAcrossPartitions, true);
                log.debug("Starting task={} lasSentPosition={}", this, segmentOffsetWriter.position(Optional.empty(), segment));
                File file = getFile();
                try {
                    int lastSentPosition = -1;
                    if (!file.exists()) {
                        log.warn("CL file={} does not exist any more, ignoring", file.getName());
                        finish(TaskStatus.SUCCESS, -1);
                        return;
                    }
                    long seg = CommitLogUtil.extractTimestamp(file.getName());

                    int currentPosition = segmentOffsetWriter.position(Optional.empty(), seg);
                    if (syncPosition >= currentPosition) {
                        CommitLogPosition minPosition = new CommitLogPosition(seg, currentPosition);
                        CommitLogReadHandlerImpl commitLogReadHandler = new CommitLogReadHandlerImpl(config, (MutationSender<TableMetadata>) mutationSender, this, currentPosition);
                        CommitLogReader commitLogReader = new CommitLogReader();
                        commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                        lastSentPosition = commitLogReadHandler.getProcessedPosition();
                    }
                    finish(TaskStatus.SUCCESS, lastSentPosition);
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
