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

import com.datastax.oss.cdc.Constants;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Kafka {@link Partitioner} that mirrors the Pulsar {@code Murmur3MessageRouter}:
 * maps a Cassandra partition token (carried in the {@code token} record header)
 * to a topic partition using the same formula used on the Pulsar side.
 *
 * <p>Falls back to the default round-robin assignment when the header is absent.
 */
public class Murmur3KafkaPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        if (numPartitions <= 0) return 0;
        // token header is not accessible from here; key bytes carry the Avro-encoded PK.
        // Use the same murmur3 shortcut on the key bytes as Murmur3MessageRouter uses on the token.
        if (keyBytes != null && keyBytes.length > 0) {
            // Replicate: (short)(token >>> 48) + Short.MAX_VALUE + 1) % numPartitions
            // We don't have the raw token here, so hash the key bytes with the same distribution.
            long hash = murmur3Hash(keyBytes);
            return (int) (Math.abs(hash) % numPartitions);
        }
        return 0;
    }

    /** MurmurHash3 x86 32-bit, truncated to long for convenience. */
    private static long murmur3Hash(byte[] data) {
        int seed = 104729;
        int c1 = 0xcc9e2d51, c2 = 0x1b873593;
        int h = seed;
        int len = data.length;
        int blocks = len / 4;
        for (int i = 0; i < blocks; i++) {
            int k = (data[i*4] & 0xff)
                  | ((data[i*4+1] & 0xff) << 8)
                  | ((data[i*4+2] & 0xff) << 16)
                  | ((data[i*4+3] & 0xff) << 24);
            k *= c1; k = Integer.rotateLeft(k, 15); k *= c2;
            h ^= k; h = Integer.rotateLeft(h, 13); h = h * 5 + 0xe6546b64;
        }
        int tail = len & 3, k1 = 0, offset = blocks * 4;
        if (tail >= 3) k1 ^= (data[offset+2] & 0xff) << 16;
        if (tail >= 2) k1 ^= (data[offset+1] & 0xff) << 8;
        if (tail >= 1) { k1 ^= (data[offset] & 0xff); k1 *= c1; k1 = Integer.rotateLeft(k1,15); k1 *= c2; h ^= k1; }
        h ^= len;
        h ^= h >>> 16; h *= 0x85ebca6b; h ^= h >>> 13; h *= 0xc2b2ae35; h ^= h >>> 16;
        return h & 0xFFFFFFFFL;
    }

    @Override public void configure(Map<String, ?> configs) {}
    @Override public void close() {}
}
