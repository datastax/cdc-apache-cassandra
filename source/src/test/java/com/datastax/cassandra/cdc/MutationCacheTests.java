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
package com.datastax.cassandra.cdc;

import com.datastax.oss.cdc.MutationCache;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MutationCacheTests {

    @Test
    public final void testMaxDigests() throws Exception {
        MutationCache mutationCache = new MutationCache(3, 10, Duration.ofHours(1));
        mutationCache.addMutationMd5("mutation1","digest1");
        mutationCache.addMutationMd5("mutation1","digest2");
        mutationCache.addMutationMd5("mutation1","digest3");
        mutationCache.addMutationMd5("mutation1","digest4");
        assertEquals(3L, mutationCache.getMutationCRCs("mutation1").size());
    }

    @Test
    public final void testIsProcessed() throws Exception {
        MutationCache mutationCache = new MutationCache(3, 10, Duration.ofHours(1));
        assertEquals(false,mutationCache.isMutationProcessed("mutation1","digest1"));
        mutationCache.addMutationMd5("mutation1","digest1");
        assertEquals(true, mutationCache.isMutationProcessed("mutation1","digest1"));
    }

    @Test
    public final void testExpireAfter() throws Exception {
        MutationCache mutationCache = new MutationCache(3, 10, Duration.ofSeconds(1));
        assertEquals(false, mutationCache.isMutationProcessed("mutation1","digest1"));
        mutationCache.addMutationMd5("mutation1","digest1");
        assertEquals(true, mutationCache.isMutationProcessed("mutation1","digest1"));
        Thread.sleep(2000);
        assertEquals(false, mutationCache.isMutationProcessed("mutation1","digest1"));
    }

}
