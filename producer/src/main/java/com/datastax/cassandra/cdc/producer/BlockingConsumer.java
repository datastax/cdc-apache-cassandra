package com.datastax.cassandra.cdc.producer;

import java.util.function.Consumer;

/**
 * A variant of {@link Consumer} that can be blocked and interrupted.
 * @param <T> the type of the input to the operation
 * @author Randall Hauch
 */
@FunctionalInterface
public interface BlockingConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws InterruptedException if the calling thread is interrupted while blocking
     */
    void accept(T t) throws InterruptedException;
}
