package com.datastax.oss.cdc.agent;

public interface CommitLogReaderInitializer {
    void initialize(AgentConfig config, CommitLogReaderService commitLogReaderService) throws Exception;
}
