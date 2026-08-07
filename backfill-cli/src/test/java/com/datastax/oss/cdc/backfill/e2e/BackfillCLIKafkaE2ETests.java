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

package com.datastax.oss.cdc.backfill.e2e;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * E2E tests for the Kafka backfill path.
 *
 * <p><b>Status: disabled — pending {@code kafka-connector} module.</b>
 *
 * <p>This test class will mirror {@link BackfillCLIE2ETests} for the Kafka platform:
 *
 * <ol>
 *   <li>Start a {@code KafkaContainer} and a {@code CassandraContainer} with the CDC agent
 *       configured for Kafka ({@code platform=KAFKA, kafkaBootstrapServers=...}).
 *   <li>Insert rows into Cassandra with CDC disabled (backfill scenario).
 *   <li>Run the backfill CLI as a subprocess:
 *       <pre>
 *       java -jar backfill-cli-all.jar \
 *         --platform KAFKA \
 *         --kafka-config-file /tmp/kafka.properties \
 *         --export-host &lt;cassandra-host&gt; \
 *         --keyspace ks1 --table table1
 *       </pre>
 *   <li>Use a {@code KafkaConsumer<byte[], byte[]>} to poll the events topic
 *       ({@code events-ks1.table1}) and assert that the expected number of Avro-encoded
 *       mutation records are received.
 * </ol>
 *
 * <p>The full pipeline verification (events topic → data topic via
 * {@code KafkaCassandraSourceTask}) requires the {@code kafka-connector} module, which
 * plays the same role as {@code CassandraSource} in the Pulsar E2E. Enable this class
 * once that module is available and the {@code e2eTest} Gradle task is wired for Kafka.
 *
 * <p>The {@code e2eTest} task in {@code backfill-cli/build.gradle} will need a Kafka variant
 * similar to the existing Pulsar block, passing:
 * <ul>
 *   <li>{@code environment 'KAFKA_IMAGE', testKafkaImage + ':' + testKafkaImageTag}
 *   <li>{@code systemProperty "agentBuildDir", project(':agent-c4').buildDir} (or c3/dse4)
 * </ul>
 */
@Slf4j
@Disabled("Pending kafka-connector module (KafkaCassandraSourceTask) — see class-level Javadoc")
public class BackfillCLIKafkaE2ETests {

    @Test
    public void testBackfillCLISinglePk() {
        // TODO: implement after kafka-connector module is available
        // 1. start KafkaContainer + CassandraContainer with Kafka agent
        // 2. create ks1.table1 with CDC disabled, insert 100 rows
        // 3. run backfill CLI subprocess with --platform KAFKA --kafka-config-file ...
        // 4. poll events-ks1.table1 with KafkaConsumer, assert 100 mutation records received
    }

    @Test
    public void testBackfillCLIFullSchema() {
        // TODO: implement after kafka-connector module is available
        // Full-schema test matching BackfillCLIE2ETests.testBackfillCLIFullSchema —
        // all CQL PK types (text, ascii, boolean, blob, timestamp, time, date, uuid,
        // timeuuid, tinyint, smallint, int, bigint, varint, decimal, double, float, inet)
    }
}
