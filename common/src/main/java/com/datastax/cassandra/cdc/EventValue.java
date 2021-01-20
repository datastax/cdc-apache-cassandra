package com.datastax.cassandra.cdc;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class EventValue {
    Long writetime;
    UUID nodeId;
    Operation operation;
}
