package com.datastax.cassandra.cdc.consumer.exceptions;

import com.datastax.cassandra.cdc.quasar.State;

public class ServiceNotRunningException extends QuasarConsumerException {
    public ServiceNotRunningException(State state) {
        super(state, "status="+state.getStatus());
    }
}
