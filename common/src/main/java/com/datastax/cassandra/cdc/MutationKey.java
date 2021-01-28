package com.datastax.cassandra.cdc;

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

    @EqualsAndHashCode.Include
    transient String id;

    public MutationKey(String keyspace, String table, String id) {
        this.keyspace = keyspace;
        this.table = table;
        this.pkColumns = null;
        this.id = id;
    }

    public MutationKey(String keyspace, String table, Object[] pkColumns) {
        this.keyspace = keyspace;
        this.table = table;
        this.pkColumns = pkColumns;
        this.id = (pkColumns.length == 1)
                ? pkColumns[0].toString()
                : "[" + Arrays.stream(pkColumns).map(x -> x.toString()).collect(Collectors.joining(",")) + "]";

    }

    public String id() {
        return this.id;
    }

    public List<Tag> tags() {
        return ImmutableList.of(Tag.of("keyspace", keyspace), Tag.of("table", table));
    }
}
