package com.datastax.cassandra.cdc.consumer.exceptions;

import com.datastax.cassandra.cdc.quasar.State;

public class HashNotManagedException extends QuasarConsumerException {

    public final Integer hash;

    public HashNotManagedException(State state, int hash) {
        super(state);
        this.hash = hash;
    }
}
