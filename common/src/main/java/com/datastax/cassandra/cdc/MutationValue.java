package com.datastax.cassandra.cdc;

import lombok.*;
import lombok.ToString;

import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class MutationValue {
    /**
     * Mutation digest
     */
    String md5Digest;

    /**
     * Source cassandra node id
     */
    UUID nodeId;

    /**
     * Optional mutated columns
     */
    String[] columns;
}
