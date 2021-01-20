package com.datastax.cassandra.cdc;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class EventValue {
    Long writetime;
    UUID nodeId;
    Operation operation;
}
