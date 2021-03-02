package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.CassandraService;
import com.datastax.cassandra.cdc.MutationKey;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.CachePut;
import io.micronaut.cache.annotation.Cacheable;

import java.util.ArrayList;
import java.util.List;


@Singleton
@CacheConfig("mutationcache")
public class MutationCache {
    private static final Logger logger = LoggerFactory.getLogger(MutationCache.class);

    final MeterRegistry meterRegistry;
    final CassandraService cassandraService;

    MutationCache(CassandraService cassandraService, MeterRegistry meterRegistry) {
        this.cassandraService = cassandraService;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Get the processed mutation CRCs in the last 10min for the given mutationKey.
     * @param mutationKey
     * @return
     */
    @Cacheable
    public List<Long> getMutationCRCs(MutationKey mutationKey) {
        return null;
    }

    @CachePut(parameters = {"mutationKey"})
    public List<Long> addMutationCRC(MutationKey mutationKey, Long crc) {
        List<Long> crcs = getMutationCRCs(mutationKey);
        if (crcs == null)
            crcs = new ArrayList<>();
        crcs.add(crc);
        return crcs;
    }

    public boolean isProcessed(MutationKey mutationKey, Long crc) {
        List<Long> crcs = getMutationCRCs(mutationKey);
        return crcs != null && crcs.contains(crc);
    }
}
