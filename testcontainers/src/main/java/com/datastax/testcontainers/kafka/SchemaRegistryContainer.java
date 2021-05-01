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
package com.datastax.testcontainers.kafka;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
    public static final String schemaRegistryContainerName = "schemaregistry";

    private SchemaRegistryContainer(String image, String boostrapServers) {
        super(image);

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", boostrapServers);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", schemaRegistryContainerName);

        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withLogConsumer(o -> {
            log.info("schemaregistry> {}", o.getUtf8String());
        });
        waitingFor(Wait.forHttp("/subjects"));
    }

    public String getRegistryUrl() {
        return "http://localhost:" + getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT);
    }

    public String getRegistryUrlInDockerNetwork() {
        return "http:/" + getContainerName() + ":" + SCHEMA_REGISTRY_INTERNAL_PORT;
    }

    public static SchemaRegistryContainer create(String image, String seed, String boostrapServers) {
        return (SchemaRegistryContainer) new SchemaRegistryContainer(image, boostrapServers)
                .withCreateContainerCmdModifier(c -> c.withName(schemaRegistryContainerName + "-" + seed));
    }
}
