package com.datastax.cassandra.cdc.consumer;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;


@ConfigurationProperties("quasar")
@Getter
public class QuasarConfiguration {
    /**
     * Quasar service DNS name.
     */
    String serviceName;

    /**
     * Node ordinal, from 0 to N-1;
     */
    Integer ordinal;

    /**
     * Quasar cluster name (Used as the PAXOS partition key).
     */
    String clusterName = "Test";

    /**
     * Cassandra datacenter name use to create the quasar keyspace.
     */
    String defaultDatacenter = "DC1";

    /**
     * Number of consumer threads
     */
    Integer consumerThreads = 4;

    public String nodeName() {
        return serviceName + "-" + ordinal;
    }
}
