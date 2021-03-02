package com.datastax.cassandra.cdc;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class MutationValue {
    Long crc;
    UUID nodeId;
    String[] columns;
}
