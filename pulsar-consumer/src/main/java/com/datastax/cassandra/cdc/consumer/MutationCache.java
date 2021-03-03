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
import java.util.HashSet;
import java.util.List;
import java.util.Set;


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
     * Get the processed mutation MD5 digest in the last 10min for the given mutationKey.
     * @param mutationKey
     * @return
     */
    @Cacheable
    public Set<String> getMutationCRCs(MutationKey mutationKey) {
        return null;
    }

    @CachePut(parameters = {"mutationKey"})
    public Set<String> addMutationMd5(MutationKey mutationKey, String md5Digest) {
        Set<String> crcs = getMutationCRCs(mutationKey);
        if (crcs == null)
            crcs = new HashSet<>();
        crcs.add(md5Digest);
        return crcs;
    }

    public boolean isProcessed(MutationKey mutationKey, String md5Digest) {
        Set<String> digests = getMutationCRCs(mutationKey);
        return digests != null && digests.contains(md5Digest);
    }
}
