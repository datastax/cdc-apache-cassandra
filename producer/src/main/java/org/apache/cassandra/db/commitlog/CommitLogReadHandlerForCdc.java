package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.db.Mutation;

public interface CommitLogReadHandlerForCdc extends CommitLogReadHandler {
    void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc, byte[] inputBuffer);
}
