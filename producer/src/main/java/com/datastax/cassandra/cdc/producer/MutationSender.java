package com.datastax.cassandra.cdc.producer;

import io.micronaut.context.annotation.DefaultImplementation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

import java.util.concurrent.CompletionStage;

@DefaultImplementation(PulsarMutationSender.class)
public interface MutationSender<P> {

    default void initialize() throws Exception {
    }

    /**
     * @return the offset of the last acknowleged mutation sent.
     */
    CommitLogPosition sentOffset();

    CompletionStage<Void> sendMutationAsync(final Mutation mutation);
}
