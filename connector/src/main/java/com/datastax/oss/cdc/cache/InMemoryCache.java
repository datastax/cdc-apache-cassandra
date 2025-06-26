package com.datastax.oss.cdc.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InMemoryCache<K> implements MutationCache<K> {
    Cache<K, List<String>> mutationCache;

    /**
     * Max number of cached digests per cached entry.
     */
    long maxDigests;

    public InMemoryCache(long maxDigests, long maxCapacity, Duration expireAfter) {
        this.maxDigests = maxDigests;
        mutationCache = Caffeine.newBuilder()
                .expireAfterWrite(expireAfter.getSeconds(), TimeUnit.SECONDS)
                .maximumSize(maxCapacity)
                .recordStats()
                .build();
    }

    public List<String> getMutationCRCs(K mutationKey) {
        return mutationCache.getIfPresent(mutationKey);
    }

    public List<String> addMutationMd5(K mutationKey, String md5Digest) {
        List<String> crcs = getMutationCRCs(mutationKey);
        if(crcs == null) {
            crcs = new ArrayList<>(1);
            crcs.add(md5Digest);
        } else {
            if (!crcs.contains(md5Digest)) {
                if (crcs.size() >= maxDigests) {
                    // remove the oldest digest
                    crcs.remove(0);
                }
                crcs.add(md5Digest);
            }
        }
        mutationCache.put(mutationKey, crcs);
        return crcs;
    }

    public boolean isMutationProcessed(K mutationKey, String md5Digest) {
        List<String> digests = getMutationCRCs(mutationKey);
        return digests != null && digests.contains(md5Digest);
    }

    public CacheStats stats() {
        return mutationCache.stats();
    }

    public long estimatedSize() {
        return mutationCache.estimatedSize();
    }
}
