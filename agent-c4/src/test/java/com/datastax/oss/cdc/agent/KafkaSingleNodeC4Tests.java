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

import com.datastax.oss.cdc.KafkaSingleNodeTests;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class KafkaSingleNodeC4Tests extends KafkaSingleNodeTests {

    public static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("CASSANDRA_IMAGE"))
                    .orElse("cassandra:" + System.getProperty("cassandraVersion"))
    ).asCompatibleSubstituteFor("cassandra");

    @Override
    public CassandraContainer<?> createCassandraContainer(
            int nodeIndex, String kafkaConfigFilePath, Network testNetwork) throws IOException {
        String agentParams = String.format(
                "platform=KAFKA,kafkaConfigFile=%s,topicPrefix=events-", kafkaConfigFilePath);
        return CassandraContainer.createCassandraContainerWithAgent(
                CASSANDRA_IMAGE, testNetwork, nodeIndex,
                System.getProperty("buildDir"), "agent-c4", agentParams, "c4");
    }
}
