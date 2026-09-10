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
 * A real Confluent Schema Registry, following the same shape as testcontainers-java's own
 * {@link org.testcontainers.containers.KafkaContainer}: a dedicated class extending
 * {@code GenericContainer<Self>}, fluent {@code with*} configuration, and a typed getter
 * ({@link #getSchemaRegistryUrl()}) hiding the host/mapped-port URL construction the same
 * way {@code KafkaContainer.getBootstrapServers()} does.
 *
 * <p>Unlike {@link ApicurioSchemaRegistryContainer}, this needs a Kafka bootstrap-servers
 * address (its storage backend is Kafka itself, not in-memory) -- see
 * {@link #withKafkaBootstrapServers(String)}, mirroring how {@code KafkaContainer} takes an
 * external dependency address via {@code withExternalZookeeper(String)}.
 *
 * <p>The server code this image runs (the {@code core} module of confluentinc/schema-registry)
 * is Confluent Community License, not Apache 2.0 -- see {@link ApicurioSchemaRegistryContainer}
 * for the Apache-2.0 alternative this project prefers; this class exists to additionally prove
 * the connector against the real thing, not as a replacement for it.
 */
public class ConfluentSchemaRegistryContainer extends GenericContainer<ConfluentSchemaRegistryContainer> {

    private static final int SCHEMA_REGISTRY_PORT = 8081;

    public ConfluentSchemaRegistryContainer(DockerImageName image) {
        super(image);
        withExposedPorts(SCHEMA_REGISTRY_PORT);
        waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
    }

    public ConfluentSchemaRegistryContainer withNetworkAlias(String alias, Network network) {
        withNetwork(network);
        withNetworkAliases(alias);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", alias);
        return this;
    }

    public ConfluentSchemaRegistryContainer withKafkaBootstrapServers(String bootstrapServers) {
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return this;
    }

    @Override
    protected void configure() {
        super.configure();
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
    }

    /** Confluent's plain REST root -- unlike Apicurio, there's no compatibility-path suffix. */
    public String getSchemaRegistryUrl() {
        return "http://" + getHost() + ":" + getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
