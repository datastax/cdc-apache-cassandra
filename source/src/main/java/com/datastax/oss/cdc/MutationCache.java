package com.datastax.oss.cdc;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Keep MD5 digests to deduplicate Cassandra mutations
 */
public class MutationCache<K> {

    Cache<K, List<String>> mutationCache;

    /**
     * Max number of cached digest per cached entry.
     */
    long maxDigests;

    public MutationCache(long maxDigests, long maxCapacity, Duration expireAfter) {
        this.maxDigests = maxDigests;
        mutationCache = Caffeine.newBuilder()
                .expireAfterWrite(expireAfter.getSeconds(), TimeUnit.SECONDS)
                .maximumSize(maxCapacity)
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
}
