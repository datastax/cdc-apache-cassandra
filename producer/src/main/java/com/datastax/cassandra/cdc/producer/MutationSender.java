package com.datastax.cassandra.cdc.producer;

import java.util.concurrent.CompletionStage;

public interface MutationSender<T> {

    default void initialize() throws Exception {
    }

    CompletionStage<Void> sendMutationAsync(final Mutation<T> mutation) throws Exception;
}
