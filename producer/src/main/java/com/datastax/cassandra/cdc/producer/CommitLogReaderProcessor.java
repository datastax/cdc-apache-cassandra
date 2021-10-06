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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public abstract class CommitLogReaderProcessor extends AbstractProcessor
{
    public static final String NAME = "CommitLogReader Processor";
    public static final String ARCHIVE_FOLDER = "archive";
    public static final String ERROR_FOLDER = "error";

    final ProducerConfig config;
    final OffsetWriter offsetWriter;
    final CommitLogTransfer commitLogTransfer;

    /**
     * COMPLETED commitlog file queue.
     */
    final PriorityBlockingQueue<File> commitLogQueue;

    public CommitLogReaderProcessor(ProducerConfig config,
                                    OffsetWriter offsetWriter,
                                    CommitLogTransfer commitLogTransfer) {
        super(NAME, 0);
        this.config = config;
        this.offsetWriter = offsetWriter;
        this.commitLogTransfer = commitLogTransfer;
        this.commitLogQueue = new PriorityBlockingQueue<>(128, CommitLogUtil::compareCommitLogs);
    }

    abstract void submitCommitLog(File file);

    @Override
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
     * Override destroy to clean up resources after stopping the processor
     */
    public void close() {
    }
}
