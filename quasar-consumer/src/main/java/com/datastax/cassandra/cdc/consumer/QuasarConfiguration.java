package com.datastax.cassandra.cdc.consumer;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.google.common.collect.Lists;
import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;

import java.util.List;


@ConfigurationProperties("quasar")
@Getter
public class QuasarConfiguration {
    /**
     * Quasar service DNS name.
     */
    String serviceName;

    /**
     * Node name ending by the ordinal index.
     */
    String nodeName;

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

    /**
     * Cassandra read before write consistency levels.
     */
    List<ConsistencyLevel> consistencies = Lists.newArrayList(ConsistencyLevel.ALL, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE);

    public int ordinal() {
        return Integer.parseInt(nodeName.substring(nodeName.lastIndexOf("-") + 1));
    }
}
