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

import com.datastax.cassandra.cdc.PulsarDualProducerTests;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Optional;

@Slf4j
public class PulsarProducerDse4Tests extends PulsarDualProducerTests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("vroyer/dse-server:6.8.14-3c3014b21ed-SNAPSHOT")
    ).asCompatibleSubstituteFor("cassandra");

    @Override
    public CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork) {
        return CassandraContainer.createCassandraContainerWithProducer(
                        CASSANDRA_IMAGE,
                        testNetwork,
                        nodeIndex,
                        System.getProperty("buildDir"),
                        "producer-dse4-pulsar",
                        String.format("pulsarServiceUrl=%s,cdcRelocationDir=/var/lib/cassandra/cdc_backup", pulsarServiceUrl),
                        "dse")
                .withEnv("DC", CassandraContainer.LOCAL_DC)
                .withContainerConfigLocation("/config");
    }

    @BeforeAll
    public static final void initBeforeClass() throws Exception { PulsarDualProducerTests.initBeforeClass(); }

    @AfterAll
    public static void closeAfterAll() {
        PulsarDualProducerTests.closeAfterAll();
    }
}
