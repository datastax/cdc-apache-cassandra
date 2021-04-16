package com.datastax.cassandra.cdc;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
    public static final String schemaRegistryContainerName = "schemaregistry";

    private SchemaRegistryContainer(String boostrapServers) {
        super("confluentinc/cp-schema-registry:" + EnhancedKafkaContainer.CONFLUENT_PLATFORM_VERSION);

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", boostrapServers);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", schemaRegistryContainerName);

        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withLogConsumer(o -> {
            log.info("schemaregistry> {}", o.getUtf8String());
        });
        waitingFor(Wait.forHttp("/subjects"));
    }

    public int getPort() {
        return SCHEMA_REGISTRY_INTERNAL_PORT;
    }

    public String getRegistryAddressInDockerNetwork() {
        return "http://" + schemaRegistryContainerName + ":" + SCHEMA_REGISTRY_INTERNAL_PORT;
    }

    public static SchemaRegistryContainer create(String boostrapServers) {
        return (SchemaRegistryContainer) new SchemaRegistryContainer(boostrapServers)
                .withCreateContainerCmdModifier(c -> c.withName(schemaRegistryContainerName));
    }
}
