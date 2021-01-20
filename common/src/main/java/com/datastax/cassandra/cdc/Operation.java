package com.datastax.cassandra.cdc;

public enum Operation {
    INSERT("i"),
    UPDATE("u"),
    DELETE("d");

    private String value;

    Operation(String value) {
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
