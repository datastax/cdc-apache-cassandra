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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class CommitLogReaderServiceTest {

    @Test
    public void finishTaskRetryTest() throws InterruptedException {
        // given
        AgentConfig config = new AgentConfig();
        config.maxInflightMessagesPerTask = 10;
        config.cdcConcurrentProcessors = 1;
        MutationSender<?> mutationSender = Mockito.mock(MutationSender.class);
        SegmentOffsetWriter segmentOffsetWriter = Mockito.mock(SegmentOffsetWriter.class);
        CommitLogTransfer commitLogTransfer = Mockito.mock(CommitLogTransfer.class);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        MyCommitLogService commitLogReaderService = new MyCommitLogService(
                config, mutationSender, segmentOffsetWriter, commitLogTransfer, executorService);
        CommitLogReaderService.Task task = commitLogReaderService.createTask("filename", 1, 1, true);

        // when
        commitLogReaderService.addPendingTask(task);

        // then
        boolean allRetriesCompleted = commitLogReaderService.getRetrylatch().await(10, TimeUnit.SECONDS); // wait for the task to be executed and retied
        assertTrue(allRetriesCompleted, "all retries should've been completed");
        executorService.shutdown();
        boolean terminated = executorService.awaitTermination(10, TimeUnit.SECONDS); // if semaphore permits where not released, awaitTermination will timeout
        assertTrue(terminated, "executor service should've been terminated");
        assertTrue(commitLogReaderService.getPendingTasks().isEmpty(), "pending tasks should be empty");
        assertEquals(3, commitLogReaderService.getAvailablePermitsRecorder().size());
        int recordPermitsCount = 0;
        while (!commitLogReaderService.getAvailablePermitsRecorder().isEmpty()) {
            recordPermitsCount++;
            int permits = commitLogReaderService.getAvailablePermitsRecorder().poll();
            if (recordPermitsCount == 3) { // this is the last recorded permit, should be 0
                assertEquals(0, permits);
            } else {
                assertEquals(config.maxInflightMessagesPerTask, permits);
            }
        }
    }

    class MyCommitLogService extends CommitLogReaderService {
        public MyCommitLogService(AgentConfig config, MutationSender<?> mutationSender, SegmentOffsetWriter segmentOffsetWriter, CommitLogTransfer commitLogTransfer, ExecutorService executorService) {
            super(config, mutationSender, segmentOffsetWriter, commitLogTransfer);
            this.tasksExecutor = executorService;

        }

        File mockFile = Mockito.mock(File.class);
        CountDownLatch retryLatch = new CountDownLatch(3); // fail twice, succeed on third retry
        BlockingQueue<Integer> availablePermitsRecorder = new LinkedBlockingDeque<>();

        @Override
        public Task createTask(String commitlogName, long seg, int pos, boolean completed) {
            return new CommitLogReaderService.Task("filename", 1, 1, true) {
                public void run() {
                    // this is where the code will block if the permits are not released properly. The idea
                    // is to limit the number of inflight messages per task, but it is up to the implementation to
                    // adhere to that.
                    inflightMessagesSemaphore.acquireUninterruptibly();
                    try {
                        if (retryLatch.getCount() > 1) {
                            lastException = new RuntimeException("failed to send to pulsar");
                        } else {
                            lastException = null;
                        }
                        Thread.sleep(50L); // noop, could be any rpc call in reality
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    inflightMessagesSemaphore.release();
                    super.finish(TaskStatus.SUCCESS, -1);
                    availablePermitsRecorder.add(inflightMessagesSemaphore.availablePermits());
                    retryLatch.countDown();
                }

                @Override
                public File getFile() {
                    return mockFile;
                }
            };
        }

        public CountDownLatch getRetrylatch() {
            return retryLatch;
        }

        public ConcurrentMap<Long, Task> getPendingTasks() {
            return pendingTasks;
        }

        public BlockingQueue<Integer> getAvailablePermitsRecorder() {
            return availablePermitsRecorder;
        }
    }
}
