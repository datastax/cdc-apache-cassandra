package com.datastax.cassandra.cdc.quasar;

public enum Status {
    UNKNOWN("UNKNOWN"),
    STARTING("STARTING"),
    JOINING("JOINING"),
    RUNNING("RUNNING"),
    LEAVING("LEAVING"),
    LEFT("LEFT");

    private String value;

    Status(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
