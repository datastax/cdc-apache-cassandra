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
     * Dirty mutation pulsar topic
     */
    String topicPrefix;
}
