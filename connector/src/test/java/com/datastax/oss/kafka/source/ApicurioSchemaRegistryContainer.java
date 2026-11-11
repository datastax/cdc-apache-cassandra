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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Apicurio Registry (Apache 2.0, server included), exposed through its
 * {@code /apis/ccompat/v7} endpoint -- a compatibility layer implementing Confluent's own wire
 * protocol, verified directly (POST .../subjects/{subject}/versions and GET
 * .../schemas/ids/{id} both behave identically to a real Confluent registry). The connector's
 * serializer (io.confluent.kafka.serializers.KafkaAvroSerializer, itself Apache 2.0) needs no
 * Apicurio-specific code to talk to it.
 *
 * <p>Follows the same shape as testcontainers-java's own
 * {@link org.testcontainers.containers.KafkaContainer}: a dedicated class extending
 * {@code GenericContainer<Self>} with a typed getter ({@link #getSchemaRegistryUrl()}) hiding
 * the host/mapped-port/path-suffix URL construction, the same way
 * {@code KafkaContainer.getBootstrapServers()} does. Unlike
 * {@link ConfluentSchemaRegistryContainer}, this needs no Kafka bootstrap-servers wiring --
 * Apicurio's default storage is in-memory, not Kafka-backed.
 */
public class ApicurioSchemaRegistryContainer extends GenericContainer<ApicurioSchemaRegistryContainer> {

    private static final int REGISTRY_PORT = 8080;
    private static final String CCOMPAT_PATH = "/apis/ccompat/v7";

    public ApicurioSchemaRegistryContainer(DockerImageName image) {
        super(image);
        withExposedPorts(REGISTRY_PORT);
        waitingFor(Wait.forHttp(CCOMPAT_PATH + "/subjects").forStatusCode(200));
    }

    public ApicurioSchemaRegistryContainer withNetworkAlias(String alias, Network network) {
        withNetwork(network);
        withNetworkAliases(alias);
        return this;
    }

    public String getSchemaRegistryUrl() {
        return "http://" + getHost() + ":" + getMappedPort(REGISTRY_PORT) + CCOMPAT_PATH;
    }
}
