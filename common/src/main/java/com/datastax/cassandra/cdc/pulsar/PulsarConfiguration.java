package com.datastax.cassandra.cdc.pulsar;


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
     * Pulsar topic
     */
    String topic;

    /**
     * Pulsar consumer subscription
     */
    @Nullable
    String subscription;
}
