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
import com.datastax.oss.cdc.PulsarDualNodeTests;
import com.datastax.testcontainers.cassandra.CassandraContainer;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.Network;

@Slf4j
public class PulsarDualNodeC5Tests extends PulsarDualNodeTests {

    public PulsarDualNodeC5Tests() {
        super(AgentTestUtil.Version.C5);
    }

    @Override
    public CassandraContainer<?> createCassandraContainer(int nodeIndex, String pulsarServiceUrl, Network testNetwork) {
        return CassandraContainer.createCassandraContainerWithAgent(
                PulsarSingleNodeC5Tests.CASSANDRA_IMAGE, testNetwork, nodeIndex, "c5", pulsarServiceUrl);
    }

}
