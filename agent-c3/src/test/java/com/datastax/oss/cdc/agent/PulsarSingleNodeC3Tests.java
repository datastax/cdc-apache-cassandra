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

import com.datastax.oss.cdc.AgentTestUtil;
import com.datastax.oss.cdc.PulsarSingleNodeTests;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class PulsarSingleNodeC3Tests extends PulsarSingleNodeTests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    public PulsarSingleNodeC3Tests() {
        super(AgentTestUtil.Version.C3);
    }

    @Override
    public CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork) {
        return CassandraContainer.createCassandraContainerWithAgent(
                CASSANDRA_IMAGE, testNetwork, nodeIndex, "c3", pulsarServiceUrl);
    }

    @Override
    public int getSegmentSize() {
        return 32 * 1024 * 1024;
    }

    @Override
    public void drain(CassandraContainer... cassandraContainers) throws IOException, InterruptedException {
        // cassandra drain to discard commitlog segments without stopping the agent
        for (CassandraContainer cassandraContainer : cassandraContainers)
            assertEquals(0, cassandraContainer.execInContainer("/opt/cassandra/bin/nodetool", "drain").getExitCode());
    }
}
