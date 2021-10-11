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
public class PulsarDualProducerV4Tests extends PulsarDualProducerTests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:4.0-beta4")
    ).asCompatibleSubstituteFor("cassandra");

    @Override
    public CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork) {
        return CassandraContainer.createCassandraContainerWithPulsarProducer(
                CASSANDRA_IMAGE, testNetwork, nodeIndex, "v4", pulsarServiceUrl);
    }

    @BeforeAll
    public static final void initBeforeClass() throws Exception { PulsarDualProducerTests.initBeforeClass(); }

    @AfterAll
    public static void closeAfterAll() {
        PulsarDualProducerTests.closeAfterAll();
    }
}
