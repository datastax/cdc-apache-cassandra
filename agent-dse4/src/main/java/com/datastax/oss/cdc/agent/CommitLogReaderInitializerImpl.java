package com.datastax.oss.cdc.agent;

import java.io.File;
import java.io.IOException;

import static com.datastax.oss.cdc.agent.CommitLogReaderService.ARCHIVE_FOLDER;
import static com.datastax.oss.cdc.agent.CommitLogReaderService.ERROR_FOLDER;

public class CommitLogReaderInitializerImpl implements CommitLogReaderInitializer {
    @Override
    public void initialize(AgentConfig config) throws Exception {
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
}
