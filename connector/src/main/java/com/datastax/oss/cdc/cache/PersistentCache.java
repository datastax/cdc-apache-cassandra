package com.datastax.oss.cdc.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class PersistentCache<K> implements MutationCache<K> {
    /**
     * The mutation cache
     */
    Cache<K, List<String>> mutationCache;

    private final TtlDB rocksDB;
    /**
     * Max number of cached digests per cached entry.
     */
    long maxDigests;

    private final Function<K, byte[]> keySerializer;

    static {
        TtlDB.loadLibrary();
    }

    public PersistentCache(long maxDigests, long maxCapacity, Duration expireAfter, String dbPath, Function<K, byte[]> keySerializer) throws RocksDBException {
        this.maxDigests = maxDigests;
        this.keySerializer = keySerializer;

        Options options = new Options().setCreateIfMissing(true);
        this.rocksDB = TtlDB.open(options, dbPath, (int) expireAfter.getSeconds(), false);

        this.mutationCache = Caffeine.newBuilder()
                .expireAfterWrite(expireAfter.getSeconds(), TimeUnit.SECONDS)
                .maximumSize(maxCapacity)
                .executor(Runnable::run)
                .recordStats()
                .removalListener((K key, List<String> value, RemovalCause cause) -> {
                    try {
                        rocksDB.delete(this.keySerializer.apply(key));
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                })
                .build();
    }


    private List<String> valueDeserializer(byte[] data){
        return data == null || data.length == 0 ? null : List.of(new String(data).split(","));
    }

    private byte[] valueSerializer(List<String> data){
        return data.stream()
                .reduce((s1, s2) -> s1 + "," + s2)
                .orElse("")
                .getBytes();
    }

    public List<String> getMutationCRCs(K mutationKey) {
        return mutationCache.get(mutationKey, k -> {
            try {
                return valueDeserializer(rocksDB.get(this.keySerializer.apply(k)));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void putMutationCRCs(K key, List<String> value) {
        mutationCache.asMap().compute(key, (k, v) -> {
            try {
                rocksDB.put(keySerializer.apply(k), valueSerializer(value));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
            return value;
        });
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
        putMutationCRCs(mutationKey, crcs);
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

    public void close() {
        rocksDB.close();
    }
}
