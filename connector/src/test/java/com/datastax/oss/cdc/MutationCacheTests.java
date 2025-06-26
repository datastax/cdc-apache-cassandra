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

import com.datastax.oss.cdc.cache.InMemoryCache;
import com.datastax.oss.cdc.cache.MutationCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MutationCacheTests {

    @Test
    public final void testMaxDigests() throws Exception {
        MutationCache<String> mutationCache = new InMemoryCache<>(3, 10, Duration.ofHours(1));
        mutationCache.addMutationMd5("mutation1","digest1");
        mutationCache.addMutationMd5("mutation1","digest2");
        mutationCache.addMutationMd5("mutation1","digest3");
        mutationCache.addMutationMd5("mutation1","digest4");
        assertEquals(3L, mutationCache.getMutationCRCs("mutation1").size());
    }

    @Test
    public final void testIsProcessed() throws Exception {
        MutationCache<String> mutationCache = new InMemoryCache<>(3, 10, Duration.ofHours(1));
        assertFalse(mutationCache.isMutationProcessed("mutation1", "digest1"));
        mutationCache.addMutationMd5("mutation1","digest1");
        assertTrue(mutationCache.isMutationProcessed("mutation1", "digest1"));
    }

    @Test
    public final void testExpireAfter() throws Exception {
        MutationCache<String> mutationCache = new InMemoryCache<>(3, 10, Duration.ofSeconds(1));
        assertFalse(mutationCache.isMutationProcessed("mutation1", "digest1"));
        mutationCache.addMutationMd5("mutation1","digest1");
        assertTrue(mutationCache.isMutationProcessed("mutation1", "digest1"));
        Thread.sleep(2000);
        assertFalse(mutationCache.isMutationProcessed("mutation1", "digest1"));
    }

    @Test
    public final void testMaxCapacity() throws Exception {
        MutationCache<String> mutationCache = new InMemoryCache<>(3, 10, Duration.ofHours(1));

        // Access and modify the private field using reflection
        Field field = InMemoryCache.class.getDeclaredField("mutationCache");
        field.setAccessible(true);
        field.set(mutationCache, Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofHours(1).getSeconds(), TimeUnit.SECONDS)
                .maximumSize(10)
                .recordStats()
                .executor(Runnable::run)    // https://github.com/ben-manes/caffeine/wiki/Testing
                .build()
        );

        for (int i = 0; i <20; i++) {
            mutationCache.addMutationMd5("mutation" + i, "digest" + i);
        }

        int count = 0;
        for (int i = 0; i < 20; i++) {
            if(mutationCache.getMutationCRCs("mutation" + i) != null) {
                count++;
            }
        }
        assertEquals(10, count);
    }

}
