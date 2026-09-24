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
import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs all {@link AbstractKafkaCassandraSourceTaskContainerTests} scenarios against the
 * OSS Apache Kafka distribution ({@code apache/kafka}).
 *
 * <p>This class is intentionally thin: it only starts the OSS broker and tells the abstract
 * base which alias to use for the CDC agent config. All test logic lives in the abstract base
 * so it is shared with {@link KafkaCassandraSourceTaskContainerTests}.
 *
 * <h3>Listener layout</h3>
 * <p>{@code org.testcontainers.kafka.KafkaContainer} (Testcontainers 1.20.x) only advertises a
 * single {@code PLAINTEXT://localhost:<mapped-port>} entry — reachable from the test host but
 * <em>not</em> from other containers on the Docker network (schema registry, CDC agent).
 *
 * <p>We therefore expose <b>two separate listeners on two separate ports</b>:
 * <ul>
 *   <li>{@code HOST://0.0.0.0:9092} → advertised as {@code HOST://localhost:<mapped-port>}.
 *       Used only by the test-process {@link org.apache.kafka.clients.consumer.KafkaConsumer}.</li>
 *   <li>{@code PLAINTEXT://0.0.0.0:29092} → advertised as {@code PLAINTEXT://kafka-oss:29092}.
 *       Used by the CDC agent (Cassandra container) and the Confluent Schema Registry container.
 *       The name <em>must</em> be {@code PLAINTEXT} because
 *       {@code SchemaRegistryConfig.endpointsToBootstrapServers} discards any entry whose
 *       protocol does not match {@code kafkastore.security.protocol} (default {@code PLAINTEXT}).
 *       </li>
 * </ul>
 */
@Slf4j
public class OssKafkaCassandraSourceTaskContainerTests extends AbstractKafkaCassandraSourceTaskContainerTests {

    private static KafkaContainer kafkaContainer;

    private static final String KAFKA_ALIAS = "kafka-oss";
    /** Port used by the PLAINTEXT (inter-container) listener — distinct from the HOST port 9092. */
    private static final int PLAINTEXT_PORT = 29092;

    @Override
    protected String kafkaNetworkAlias() {
        return KAFKA_ALIAS;
    }

    /**
     * The CDC agent runs inside the Cassandra container on the Docker network; it must reach the
     * broker via the inter-container {@code PLAINTEXT} listener on {@value #PLAINTEXT_PORT}.
     */
    @Override
    protected String agentBootstrapAddress() {
        return KAFKA_ALIAS + ":" + PLAINTEXT_PORT;
    }

    /**
     * The schema registry container connects to Kafka over the Docker network.  It must use the
     * {@code PLAINTEXT} protocol name (required by {@code SchemaRegistryConfig}) and the
     * inter-container port {@value #PLAINTEXT_PORT}.
     */
    @Override
    protected String schemaRegistryBootstrapUri() {
        return "PLAINTEXT://" + KAFKA_ALIAS + ":" + PLAINTEXT_PORT;
    }

    @Override
    protected KafkaConsumerHandle startKafkaContainer(Network network) {
        // Two listeners on two ports so the test process and Docker-network clients each get a
        // reachable address:
        //
        //  HOST     0.0.0.0:9092   → advertised as HOST://localhost:<mapped>   (test process)
        //  PLAINTEXT 0.0.0.0:29092 → advertised as PLAINTEXT://kafka-oss:29092 (containers)
        //  BROKER   0.0.0.0:9093   → advertised as BROKER://<hostname>:9093    (inter-broker)
        //  CONTROLLER 0.0.0.0:9094 → controller quorum (KRaft)
        //
        // containerIsStarting() is overridden to rewrite the starter script's
        // KAFKA_ADVERTISED_LISTENERS export so all four entries are present.  The parent's
        // implementation only writes HOST + BROKER; we regenerate the whole script here.
        kafkaContainer = new KafkaContainer(AgentTestUtil.OSS_KAFKA_IMAGE) {
            @Override
            protected void containerIsStarting(InspectContainerResponse containerInfo) {
                String brokerAdvertisedListener = String.format(
                        "BROKER://%s:9093", containerInfo.getConfig().getHostName());

                List<String> advertisedListeners = new ArrayList<>();
                // HOST listener: the parent's getBootstrapServers() returns localhost:<mapped-port>
                advertisedListeners.add("HOST://" + getBootstrapServers());
                advertisedListeners.add(brokerAdvertisedListener);
                advertisedListeners.add("PLAINTEXT://" + KAFKA_ALIAS + ":" + PLAINTEXT_PORT);
                String kafkaAdvertisedListeners = String.join(",", advertisedListeners);

                String command = "#!/bin/bash\n";
                command += "export KAFKA_ADVERTISED_LISTENERS=" + kafkaAdvertisedListeners + "\n";
                command += "/etc/kafka/docker/run\n";
                copyFileToContainer(Transferable.of(command, 0777), "/tmp/testcontainers_start.sh");
            }
        }
                .withNetwork(network)
                .withNetworkAliases(KAFKA_ALIAS)
                .withEnv("KAFKA_LISTENERS",
                        "HOST://0.0.0.0:9092"
                                + ",PLAINTEXT://0.0.0.0:" + PLAINTEXT_PORT
                                + ",BROKER://0.0.0.0:9093"
                                + ",CONTROLLER://0.0.0.0:9094")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

        kafkaContainer.start();

        // The test-process KafkaConsumer connects via the HOST listener (host-mapped port).
        return new KafkaConsumerHandle() {
            @Override public String bootstrapServers() { return kafkaContainer.getBootstrapServers(); }
            @Override public void close() { kafkaContainer.close(); }
        };
    }

    @BeforeAll
    static void startContainers() throws Exception {
        startContainersImpl(new OssKafkaCassandraSourceTaskContainerTests());
    }
}
