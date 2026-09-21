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
package com.datastax.oss.kafka.source;

import com.datastax.oss.cdc.AgentTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;

/**
 * Runs all {@link AbstractKafkaCassandraSourceTaskContainerTests} scenarios against the
 * Confluent Kafka distribution ({@code confluentinc/cp-kafka}).
 *
 * <p>This class is intentionally thin: it only starts the Confluent broker and tells the
 * abstract base which alias to use for the CDC agent config. All test logic lives in the
 * abstract base so it is shared with {@link OssKafkaCassandraSourceTaskContainerTests}.
 */
@Slf4j
public class KafkaCassandraSourceTaskContainerTests extends AbstractKafkaCassandraSourceTaskContainerTests {

    // cp-kafka uses the legacy KafkaContainer (org.testcontainers.containers) whose startup
    // script calls /etc/confluent/docker/run — distinct from the new org.testcontainers.kafka
    // KafkaContainer which only works with apache/kafka images.
    private static org.testcontainers.containers.KafkaContainer kafkaContainer;

    @Override
    protected String kafkaNetworkAlias() {
        return "kafka-confluent";
    }

    @Override
    protected KafkaConsumerHandle startKafkaContainer(Network network) {
        kafkaContainer = new org.testcontainers.containers.KafkaContainer(AgentTestUtil.CONFLUENT_KAFKA_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("kafka-confluent");
        kafkaContainer.start();
        return new KafkaConsumerHandle() {
            @Override public String bootstrapServers() { return kafkaContainer.getBootstrapServers(); }
            @Override public void close() { kafkaContainer.close(); }
        };
    }

    @BeforeAll
    static void startContainers() throws Exception {
        startContainersImpl(new KafkaCassandraSourceTaskContainerTests());
    }
}
