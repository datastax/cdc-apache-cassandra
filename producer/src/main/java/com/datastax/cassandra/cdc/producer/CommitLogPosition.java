/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    public static final CommitLogPosition NULL_POSITION = new CommitLogPosition(-1, 0);

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
