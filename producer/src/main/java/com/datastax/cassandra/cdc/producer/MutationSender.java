package com.datastax.cassandra.cdc.producer;

import java.util.concurrent.CompletionStage;

public interface MutationSender<T> {

    default void initialize() throws Exception {
    }

    /**
     * @return the offset of the last acknowleged mutation sent.
     */
    CommitLogPosition sentOffset();

    CompletionStage<Void> sendMutationAsync(final Mutation<T> mutation) throws Exception;
}
