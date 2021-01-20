package com.datastax.cassandra.cdc;

import lombok.*;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

@Getter
@Setter
@Builder
@With
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class EventKey {
    UUID node;
    String keyspace;
    String table;
    Object[] pkColumns;

    public String id() {
        if (pkColumns.length == 1) {
            return pkColumns[0].toString();
        } else {
            return "[" + Arrays.stream(pkColumns).map(x -> x.toString()).collect(Collectors.joining(",")) + "]";
        }
    }
}
