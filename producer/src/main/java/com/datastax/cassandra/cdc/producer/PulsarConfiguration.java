package com.datastax.cassandra.cdc.producer;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;

import javax.annotation.Nullable;


@ConfigurationProperties("pulsar")
@Getter
public class PulsarConfiguration {

    /**
     * Pulsar producer/consumer name
     */
    String name;

    /**
     * pulsar serviceUrl
     */
    String serviceUrl;

    /**
     * Dirty cassandra row pulsar topic
     */
    String topic;

    /**
     * Pulsar sink topic
     */
    String sinkTopic;

}
