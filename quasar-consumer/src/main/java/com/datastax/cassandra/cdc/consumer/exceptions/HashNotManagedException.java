package com.datastax.cassandra.cdc.consumer.exceptions;

import com.datastax.cassandra.cdc.quasar.State;

public class HashNotManagedException extends QuasarConsumerException {

    public final Integer hash;

    public HashNotManagedException(State state, int hash) {
        this(state, hash, "hash=" + hash + " size=" + state.getSize());
    }

    public HashNotManagedException(State state, int hash, String msg) {
        super(state, msg);
        this.hash = hash;
    }
}
