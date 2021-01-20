package com.datastax.cassandra.cdc.producer;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;

import javax.inject.Singleton;
import javax.management.ServiceNotFoundException;

@Factory
public class CommitLogTransferFactory {

    @Singleton
    @Bean
    public CommitLogTransfer getCommitLogTransfer() throws ServiceNotFoundException {
        //TODO: make this configurable
        return new BlackHoleCommitLogTransfer();
    }
}