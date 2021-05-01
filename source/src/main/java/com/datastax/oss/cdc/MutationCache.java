/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
