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

import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.time.Duration;
import java.util.Arrays;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Detect and read commitlogs files.
 *
 * @author vroyer
 */
@Slf4j
public class CommitLogProcessor extends AbstractProcessor implements AutoCloseable {
    private static final String NAME = "Commit Log Processor";

    private final CommitLogTransfer commitLogTransfer;
    private final File cdcDir;
    private final AbstractDirectoryWatcher newCommitLogWatcher;
    private boolean initial = true;

    CommitLogReaderProcessor commitLogReaderProcessor;
    OffsetWriter offsetWriter;
    ProducerConfig config;

    public CommitLogProcessor(String cdcLogDir,
                              ProducerConfig config,
                              CommitLogTransfer commitLogTransfer,
                              OffsetWriter offsetWriter,
                              CommitLogReaderProcessor commitLogReaderProcessor) throws IOException {
        super(NAME, 0);
        this.config = config;
        this.commitLogReaderProcessor = commitLogReaderProcessor;
        this.commitLogTransfer = commitLogTransfer;
        this.offsetWriter = offsetWriter;
        this.cdcDir = new File(cdcLogDir);
        this.newCommitLogWatcher = new AbstractDirectoryWatcher(cdcDir.toPath(),
                Duration.ofMillis(config.cdcDirPollIntervalMs),
                ImmutableSet.of(ENTRY_CREATE, ENTRY_MODIFY)) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) throws IOException {
                if (path.toString().endsWith(".log")) {
                    commitLogReaderProcessor.submitCommitLog(path.toFile());
                }
                if (path.toString().endsWith("_cdc.idx")) {
                    commitLogReaderProcessor.submitCommitLog(path.toFile());
                }
            }
        };

    }



    /**
     * Override destroy to clean up resources after stopping the processor
     */
    @Override
    public void close() {
    }

    @Override
    public void process() throws IOException, InterruptedException {
        if (config.errorCommitLogReprocessEnabled) {
            log.debug("Moving back error commitlogs for reprocessing");
            commitLogTransfer.getErrorCommitLogFiles();
        }

        // load existing commitlogs files when initializing
        if (initial) {
            File[] commitLogFiles = CommitLogUtil.getCommitLogs(cdcDir);
            log.debug("Reading existing commit logs in {}, files={}", cdcDir, Arrays.asList(commitLogFiles));
            Arrays.sort(commitLogFiles, CommitLogUtil::compareCommitLogs);
            File youngerCdcIdxFile = null;
            for (File file : commitLogFiles) {
                // filter out already processed commitlogs
                long segmentId = CommitLogUtil.extractTimestamp(file.getName());
                if (file.getName().endsWith(".log")) {
                    // only submit logs, not _cdc.idx
                    if(segmentId >= offsetWriter.offset().segmentId) {
                        commitLogReaderProcessor.submitCommitLog(file);
                    }
                } else if (file.getName().endsWith("_cdc.idx")) {
                    if (youngerCdcIdxFile == null ||  segmentId > CommitLogUtil.extractTimestamp(youngerCdcIdxFile.getName())) {
                        youngerCdcIdxFile = file;
                    }
                }
            }
            if (youngerCdcIdxFile != null) {
                // init the last synced position
                log.debug("Read last synced position from file={}", youngerCdcIdxFile);
                commitLogReaderProcessor.submitCommitLog(youngerCdcIdxFile);
            }
            initial = false;
        }

        // collect new segment files
        newCommitLogWatcher.poll();
    }
}
