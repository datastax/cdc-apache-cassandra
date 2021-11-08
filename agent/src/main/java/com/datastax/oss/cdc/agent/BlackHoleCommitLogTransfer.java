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

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Implementation of {@link CommitLogTransfer} which deletes commit logs.
 */
@Slf4j
public class BlackHoleCommitLogTransfer implements CommitLogTransfer {

    AgentConfig config;

    BlackHoleCommitLogTransfer(AgentConfig config) {
        this.config = config;
    }

    @Override
    public void onSuccessTransfer(Path file) {
        deleteCommitlog(file);
    }

    @Override
    public void onErrorTransfer(Path file) {
        deleteCommitlog(file);
    }

    void deleteCommitlog(Path file) {
        try {
            Files.delete(file);
            String filename = file.getFileName().toString();
            String cdcIdxFilename = filename.substring(0, filename.length() - ".log".length()) + "_cdc.idx";
            File cdcIdxFile = new File(file.getParent().toString(), cdcIdxFilename);
            if (cdcIdxFile.exists()) {
                cdcIdxFile.delete();
            }
        } catch (IOException e) {
            log.error("error:", e);
        }
    }

    /**
     * Move CL on error to the cdc directory
     */
    @Override
    public void recycleErrorCommitLogFiles(Path cdcDir) {
        for(File file : CommitLogUtil.getCommitLogs(Paths.get(config.cdcWorkingDir, CommitLogReaderService.ERROR_FOLDER).toFile())) {
            CommitLogUtil.moveCommitLog(file, cdcDir);
        }
    }
}
