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
package com.datastax.oss.cdc.cache;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.List;

public interface MutationCache<K> {
    /**
     * Add a mutation MD5 digest to the cache for the given mutation key.
     * @param mutationKey the key for the mutation, typically a partition key or a unique identifier
     * @param md5Digest the MD5 digest of the mutation to be added
     * @return a list of MD5 digests for the given mutation key, which may include the newly added digest
     */
    List<String> addMutationMd5(K mutationKey, String md5Digest);

    /**
     * Retrieve the list of MD5 digests for the given mutation key.
     * @param mutationKey the key for the mutation
     * @return a list of MD5 digests associated with the mutation key, or an empty list if none exist
     */
    List<String> getMutationCRCs(K mutationKey);

    /**
     * Check if a mutation with the given key and MD5 digest has already been processed.
     * @param mutationKey the key for the mutation
     * @param md5Digest the MD5 digest of the mutation
     * @return true if the mutation has been processed, false otherwise
     */
    boolean isMutationProcessed(K mutationKey, String md5Digest);

    /**
     * Gives the current statistics of the cache, such as hit rate, miss rate, and size.
     * <a href="https://github.com/ben-manes/caffeine/wiki/Statistics">Caffeine Statistics wiki</a>
     */
    CacheStats stats();

    /**
     * Gives the estimated size of the cache.
     */
    long estimatedSize();
}
