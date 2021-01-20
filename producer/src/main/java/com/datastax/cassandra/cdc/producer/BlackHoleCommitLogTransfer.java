/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.datastax.cassandra.cdc.producer;

import java.io.File;
import java.util.Properties;

/**
 * Implementation of {@link CommitLogTransfer} which deletes commit logs.
 */
public class BlackHoleCommitLogTransfer implements CommitLogTransfer {

    @Override
    public void onSuccessTransfer(File file) {
        CommitLogUtil.deleteCommitLog(file);
    }

    @Override
    public void onErrorTransfer(File file) {
        CommitLogUtil.deleteCommitLog(file);
    }

    @Override
    public void getErrorCommitLogFiles() {
    }
}
