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

import lombok.NoArgsConstructor;

import java.util.UUID;

@NoArgsConstructor
public abstract class AbstractMutationMaker<T, M> {

    public void insert(UUID node, long segment, int position,
                       long tsMicro, Object[] pkValues, BlockingConsumer<M> consumer,
                       String md5Digest, T t, Object token) {
        createRecord(node, segment, position, tsMicro, pkValues, consumer, md5Digest, t, token);
    }

    public void update(UUID node, long segment, int position,
                       long tsMicro, Object[] pkValues, BlockingConsumer<M> consumer,
                       String md5Digest, T t, Object token) {
        createRecord(node, segment, position, tsMicro, pkValues, consumer, md5Digest, t, token);
    }

    public void delete(UUID node, long segment, int position,
                       long tsMicro, Object[] pkValues, BlockingConsumer<M> consumer,
                       String md5Digest, T t, Object token) {
        createRecord(node, segment, position, tsMicro, pkValues, consumer, md5Digest, t, token);
    }

    public abstract void createRecord(UUID nodeId, long segment, int position,
                              long tsMicro, Object[] pkValues, BlockingConsumer<M> consumer,
                              String md5Digest, T t, Object token);
}
