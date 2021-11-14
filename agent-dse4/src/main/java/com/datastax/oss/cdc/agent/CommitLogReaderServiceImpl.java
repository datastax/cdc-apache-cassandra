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

import com.datastax.oss.cdc.agent.exceptions.CassandraConnectorSchemaException;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.schema.TableMetadata;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.IntBinaryOperator;

/**
 * Consume a queue of commitlog files to read mutations.
 */
@Slf4j
public class CommitLogReaderServiceImpl extends CommitLogReaderService {

    public CommitLogReaderServiceImpl(AgentConfig config,
                                      MutationSender<TableMetadata> mutationSender,
                                      SegmentOffsetWriter segmentOffsetWriter,
                                      CommitLogTransfer commitLogTransfer) {
        super(config, mutationSender, segmentOffsetWriter, commitLogTransfer);
        this.tasksExecutor = JMXEnabledThreadPoolExecutor.createAndPrestart(
                config.cdcConcurrentProcessors == -1 ? DatabaseDescriptor.getFlushWriters() : config.cdcConcurrentProcessors,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("CdcCommitlogProcessor"),
                "internal");
    }

    @SuppressWarnings("unchecked")
    public Task createTask(String filename, long segment, int syncPosition, boolean completed) {
        return new Task(filename, segment, syncPosition, completed) {
            CommitLogReadHandlerImpl commitLogReadHandlerImpl;
            int maxPosition = 0;

            public void run() {
                log.debug("Starting task={} lasSentPosition={}", this, segmentOffsetWriter.position(Optional.empty(), segment));
                File file = getFile();
                try {
                    if (!file.exists()) {
                        log.warn("CL file={} does not exist any more, ignoring", file.getName());
                        finish(TaskStatus.SUCCESS, -1);
                        return;
                    }
                    long seg = CommitLogUtil.extractTimestamp(file.getName());
                    int currentPosition = segmentOffsetWriter.position(Optional.empty(), seg);
                    if (syncPosition > currentPosition) {
                        commitLogReadHandlerImpl = new CommitLogReadHandlerImpl(this::sendAsync);
                        CommitLogPosition minPosition = new CommitLogPosition(seg, currentPosition);
                        CommitLogReader commitLogReader = new CommitLogReader();
                        commitLogReader.readCommitLogSegment(commitLogReadHandlerImpl, file, minPosition, false);
                    }
                    finish(TaskStatus.SUCCESS, maxPosition);
                } catch (Exception e) {
                    log.warn("Task failed {}", this, e);
                    finish(TaskStatus.ERROR, -1);
                } finally {
                    CdcMetrics.executedTasks.inc();
                }
            }

            @Override
            public File getFile() {
                return new File(DatabaseDescriptor.getCDCLogLocation(), filename);
            }

            public CompletableFuture<?> sendAsync(AbstractMutation<TableMetadata> mutation) {
                log.debug("Sending mutation={}", mutation);
                try {
                    inflightMessagesSemaphore.acquireUninterruptibly(); // may block
                    CompletableFuture<?> future = ((MutationSender<TableMetadata>) mutationSender).sendMutationAsync(mutation)
                            .handle((msgId, t)-> {
                                if (t == null) {
                                    CdcMetrics.sentMutations.inc();
                                    log.debug("Sent mutation={}", mutation);
                                } else {
                                    if (t instanceof CassandraConnectorSchemaException) {
                                        log.error("Invalid primary key schema:", t);
                                        CdcMetrics.skippedMutations.inc();
                                    } else {
                                        CdcMetrics.sentErrors.inc();
                                        log.debug("Sent failed mutation=" + mutation, t);
                                        lastException = t;
                                    }
                                }
                                inflightMessagesSemaphore.release();
                                return msgId;
                            });
                    maxPosition = Math.max(maxPosition, mutation.getPosition());
                    return future;
                } catch(Exception e) {
                    log.error("Send failed:", e);
                    CdcMetrics.sentErrors.inc();
                    CompletableFuture<?> future = new CompletableFuture<>();
                    future.completeExceptionally(e);
                    return future;
                }
            }
        };
    }
}
