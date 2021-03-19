package com.datastax.cassandra.cdc.converter;


import org.apache.pulsar.client.api.Schema;

/**
 * Converters help to change the format of data from one format into another format.
 * Converters are decoupled from connectors to allow reuse of converters between connectors naturally.
 */
public interface Converter<V, R, T> {

    Schema<V> getSchema();

    /**
     * Convert the connector representation to the Pulsar internal representation.
     * @param r
     * @return
     */
     V toConnectData(R r);

    /**
     * Decode the pulsar IO internal representation to the connector representation.
     * @return
     */
     T fromConnectData(V value);
}
