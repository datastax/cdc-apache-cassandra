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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;

import java.io.File;

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
    }

    public Task createTask(String filename, long segment, int syncPosition, boolean completed) {
        return new Task(filename, segment, syncPosition, true) {
            public void run() {
                File file = new File(DatabaseDescriptor.getCDCLogLocation(), filename);
                if (!file.exists()) {
                    log.debug("file={} does not exist any more, ignoring", file.getName());
                    return;
                }
                long seg = CommitLogUtil.extractTimestamp(file.getName());

                CommitLogReader commitLogReader = new CommitLogReader();
                try {
                    // hack to use a dummy min position for segment ahead of the offsetFile.
                    int minPosition = segmentOffsetWriter.position(seg);
                    commitLogReader.readCommitLogSegment(commitLogReadHandler, file, minPosition, false);
                    log.debug("Successfully processed commitlog completed={} minPosition={} file={}",
                            completed, minPosition, file.getName());

                    // do not transfer the active commitlog on Cassandra 4.x
                    commitLogTransfer.onSuccessTransfer(file.toPath());
                    segmentOffsetWriter.remove(seg);
                } catch (Exception e) {
                    log.warn("Failed to read commitlog completed=" + completed + " file=" + file.getName(), e);
                    if (completed) {
                        // do not transfer the active commitlog on Cassandra 4.x
                        commitLogTransfer.onErrorTransfer(file.toPath());
                    }
                }
            }
        };
    }
}
