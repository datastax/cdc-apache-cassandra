package com.datastax.cassandra.cdc.producer;

import java.io.IOException;

/**
 * Periodically persist the last sent offset to recover from that checkpoint.
 */
public interface OffsetWriter {

    public void maybeCommitOffset(Mutation<?> record);

    /**
     * Increment the number of mutation not acked.
     * @return
     */
    public long incNotCommittedEvents();

    /**
     * Set the current offset.
     * @param sourceOffset
     */
    public void markOffset(CommitLogPosition sourceOffset);

    /**
     * Get the current offset.
     * @return
     */
    public CommitLogPosition offset();

    /**
     * Persist the offset
     * @throws IOException
     */
    public void flush() throws IOException;

}
