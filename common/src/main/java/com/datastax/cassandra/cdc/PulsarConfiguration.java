package com.datastax.cassandra.cdc;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;

import javax.annotation.Nullable;


@ConfigurationProperties("pulsar")
@Getter
public class PulsarConfiguration {

    String serviceUrl;

    String topic;

    @Nullable
    String subscription;
}
