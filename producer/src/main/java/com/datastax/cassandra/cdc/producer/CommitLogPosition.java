package com.datastax.cassandra.cdc.producer;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Comparator;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class CommitLogPosition implements Comparable<CommitLogPosition> {
    public final long segmentId;
    public final int position;

    public static final Comparator<CommitLogPosition> comparator = new Comparator<CommitLogPosition>()
    {
        public int compare(CommitLogPosition o1, CommitLogPosition o2)
        {
            if (o1.segmentId != o2.segmentId)
                return Long.compare(o1.segmentId,  o2.segmentId);

            return Integer.compare(o1.position, o2.position);
        }
    };

    public int compareTo(CommitLogPosition other)
    {
        return comparator.compare(this, other);
    }
}
