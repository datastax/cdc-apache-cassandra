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
    Object[] columns;

    public String id() {
        if (columns.length == 1) {
            return columns[0].toString();
        } else {
            return "[" + Arrays.stream(columns).map(x -> x.toString()).collect(Collectors.joining(",")) + "]";
        }
    }
}
