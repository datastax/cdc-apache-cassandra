package com.datastax.cassandra.cdc.consumer.exceptions;

import com.datastax.cassandra.cdc.quasar.State;

public abstract class QuasarConsumerException extends Exception {
    public final State state;

    public QuasarConsumerException(State state) {
        this.state = state;
    }
}
