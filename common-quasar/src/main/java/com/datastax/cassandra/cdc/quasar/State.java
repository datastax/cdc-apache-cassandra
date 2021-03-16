package com.datastax.cassandra.cdc.quasar;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@ToString(includeFieldNames=true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@With
@Builder(toBuilder=true)
public class State {
    public static final State NO_STATE = new State(Status.UNKNOWN, -1,-1L);

    Status status;
    Integer size;
    Long version;

    public State(@JsonProperty("status") Status status,
                 @JsonProperty("size") int size,
                 @JsonProperty("version") long version) {
        this.status = status;
        this.size = size;
        this.version = version;
    }
}
