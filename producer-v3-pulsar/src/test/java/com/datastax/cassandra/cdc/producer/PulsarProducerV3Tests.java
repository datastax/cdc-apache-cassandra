/**
 * Copyright DataStax, Inc 2021.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.PulsarProducerTests;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarProducerV3Tests extends PulsarProducerTests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE")).orElse("cassandra:3.11.10")
    ).asCompatibleSubstituteFor("cassandra");

    @Override
    public CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork) {
        return CassandraContainer.createCassandraContainerWithPulsarProducer(
                CASSANDRA_IMAGE, testNetwork, nodeIndex, "v3", pulsarServiceUrl);
    }

    @Override
    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // cassandra drain to discard commitlog segments without stopping the producer
        for (CassandraContainer cassandraContainer : cassandraContainers)
            assertEquals(0, cassandraContainer.execInContainer("/opt/cassandra/bin/nodetool", "drain").getExitCode());
    }

    @BeforeAll
    public static final void initBeforeClass() throws Exception {
        PulsarProducerTests.initBeforeClass();
    }

    @AfterAll
    public static void closeAfterAll() {
        PulsarProducerTests.closeAfterAll();
    }
}
