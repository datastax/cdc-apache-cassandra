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

package com.datastax.oss.cdc.backfill;

/**
 * Dictates the cassandra image & agent to use for the test.
 */
public enum CassandraFamily {
    C3,     // Cassandra 3.11.x + agent-c3
    C4,     // Cassandra 4.x + agent-c4
    DSE4    // Datastax Enterprise Server + agent-dse4
}
