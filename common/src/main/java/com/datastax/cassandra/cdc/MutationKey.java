package com.datastax.cassandra.cdc;

import com.datastax.cassandra.cdc.quasar.Murmur3HashFunction;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Tag;
import lombok.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class MutationKey {
    String keyspace;
    String table;

    @EqualsAndHashCode.Exclude
    Object[] pkColumns;

    public MutationKey(String keyspace, String table, Object[] pkColumns) {
        this.keyspace = keyspace;
        this.table = table;
        this.pkColumns = pkColumns;
    }

    public String id() {
        return (pkColumns.length == 1)
                ? pkColumns[0].toString()
                : "[" + Arrays.stream(pkColumns).map(x -> x.toString()).collect(Collectors.joining(",")) + "]";
    }

    public int hash() {
        return Murmur3HashFunction.hash(id());
    }

    public List<Tag> tags() {
        return ImmutableList.of(Tag.of("keyspace", keyspace), Tag.of("table", table));
    }

    // TODO: parse compound key.
    public MutationKey parseId() {
        this.pkColumns = new Object[] { id() };
        return this;
    }
}
