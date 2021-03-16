package com.datastax.cassandra.cdc;

import lombok.*;

import java.util.Arrays;
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

    // TODO: parse compound key.
    public MutationKey parseId() {
        this.pkColumns = new Object[] { id() };
        return this;
    }
}
