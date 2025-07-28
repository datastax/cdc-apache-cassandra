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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class PersistentCacheTests {
    private String path;

    @BeforeEach
    public void setUp() {
        path = "./rocksdb_mutation_cache_" + (new Random()).nextInt();
    }

    @AfterEach
    public void tearDown() {
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (Exception e) {
            // Ignore any exceptions during cleanup
        }
    }

    @Test
    public final void testMaxDigests() throws Exception {
        MutationCache<String> mutationCache = new PersistentCache<>(3, 10, Duration.ofHours(1), path, String::getBytes);
        mutationCache.addMutationMd5("mutation1","digest1");
        mutationCache.addMutationMd5("mutation1","digest2");
        mutationCache.addMutationMd5("mutation1","digest3");
        mutationCache.addMutationMd5("mutation1","digest4");
        assertEquals(3L, mutationCache.getMutationCRCs("mutation1").size());
    }

    @Test
    public final void testIsProcessed() throws Exception {
        MutationCache<String> mutationCache = new PersistentCache<>(3, 10, Duration.ofHours(1), path, String::getBytes);
        assertFalse(mutationCache.isMutationProcessed("mutation1", "digest1"));
        mutationCache.addMutationMd5("mutation1","digest1");
        assertTrue(mutationCache.isMutationProcessed("mutation1", "digest1"));

    }

    @Test
    public final void testPersistence() throws Exception {
        PersistentCache<String> mutationCache = new PersistentCache<>(3, 10, Duration.ofHours(1), path, String::getBytes);
        mutationCache.addMutationMd5("mutation1","digest1");
        mutationCache.addMutationMd5("mutation1","digest2");
        mutationCache.addMutationMd5("mutation1","digest3");

        mutationCache.close();
        mutationCache = new PersistentCache<>(3, 10, Duration.ofHours(1), path, String::getBytes);
        assertEquals(3L, mutationCache.getMutationCRCs("mutation1").size());
    }

    @Test
    public final void testMaxCapacity() throws Exception {
        MutationCache<String> mutationCache = new PersistentCache<>(3, 10, Duration.ofHours(1), path, String::getBytes);

        for (int i = 0; i <20; i++) {
            mutationCache.addMutationMd5("mutation" + i, "digest" + i);
        }

        for (int i = 0; i < 20; i++) {
            List<String> expected = new java.util.ArrayList<>();
            expected.add("digest" + i);
            assertEquals(expected, mutationCache.getMutationCRCs("mutation" + i));
        }
    }
}
