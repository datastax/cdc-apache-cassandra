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
package com.datastax.oss.cdc.agent;

import com.google.common.hash.Hashing;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Murmur3KafkaPartitionerTest {

    private static final String TOPIC = "events-ks.tbl";
    private static final int SEED = 104729;

    private final Murmur3KafkaPartitioner partitioner = new Murmur3KafkaPartitioner();

    private static Cluster clusterWithPartitions(int count) {
        Node node = new Node(0, "localhost", 9092);
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new PartitionInfo(TOPIC, i, node, new Node[]{node}, new Node[]{node}));
        }
        return new Cluster("test", Collections.singletonList(node), partitions,
                Collections.emptySet(), Collections.emptySet());
    }

    /** Reference partition: Guava's MurmurHash3 x86 32-bit with the same seed, as unsigned. */
    private static int expectedPartition(byte[] keyBytes, int numPartitions) {
        long unsignedHash = Hashing.murmur3_32_fixed(SEED).hashBytes(keyBytes).asInt() & 0xFFFFFFFFL;
        return (int) (unsignedHash % numPartitions);
    }

    // Lengths 0 to 9 cover full 4-byte blocks and every tail length (1, 2 and 3 bytes).
    @ParameterizedTest
    @ValueSource(strings = {"a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", "abcdefgh", "partition-key-1"})
    void matchesReferenceMurmur3(String key) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        Cluster cluster = clusterWithPartitions(7);
        assertEquals(expectedPartition(keyBytes, 7),
                partitioner.partition(TOPIC, key, keyBytes, null, null, cluster));
    }

    @Test
    void sameKeyAlwaysMapsToSamePartition() {
        byte[] keyBytes = {1, 2, 3, 4, 5};
        Cluster cluster = clusterWithPartitions(16);
        int first = partitioner.partition(TOPIC, null, keyBytes, null, null, cluster);
        for (int i = 0; i < 10; i++) {
            assertEquals(first, partitioner.partition(TOPIC, null, keyBytes.clone(), null, null, cluster));
        }
        assertTrue(first >= 0 && first < 16);
    }

    @Test
    void missingOrEmptyKeyGoesToPartitionZero() {
        Cluster cluster = clusterWithPartitions(4);
        assertEquals(0, partitioner.partition(TOPIC, null, null, null, null, cluster));
        assertEquals(0, partitioner.partition(TOPIC, null, new byte[0], null, null, cluster));
    }

    @Test
    void configureAndCloseAreNoOps() {
        partitioner.configure(Collections.emptyMap());
        partitioner.close();
        Cluster cluster = clusterWithPartitions(3);
        byte[] keyBytes = {9, 9};
        assertEquals(expectedPartition(keyBytes, 3), partitioner.partition(TOPIC, null, keyBytes, null, null, cluster));
    }
}
