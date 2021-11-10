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
package com.datastax.oss.cdc.agent;

import com.datastax.oss.cdc.MutationValue;
import lombok.*;

import java.util.List;
import java.util.UUID;


/**
 * An immutable data structure representing a change event.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public abstract class AbstractMutation<T> implements TableInfo {
    private UUID nodeId;
    private long segment;
    private int position;
    private Object[] pkValues;
    private long ts;
    private String md5Digest;
    protected T metadata;
    private Object token;

    public abstract String key();
    public abstract String name();
    public abstract String keyspace();
    public abstract List<ColumnInfo> primaryKeyColumns();

    public MutationValue mutationValue() {
        // TODO: Unfortunately, computing the mutation CRC require to re-serialize it because we cannot get the byte[] from the commitlog reader.
        // So, we use the timestamp here.
        return new MutationValue(md5Digest, nodeId, null);
    }
}
