package com.datastax.cassandra.cdc;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

    public static final int KAFKA_CONNECT_INTERNAL_PORT = 8083;
    public static final String kafkaConnectContainerName = "connect";

    private KafkaConnectContainer(String image, String boostrapServers, String zookeeperUrl) {
        super(image);

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", boostrapServers);
        addEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperUrl);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", kafkaConnectContainerName);

        withExposedPorts(KAFKA_CONNECT_INTERNAL_PORT);
        withLogConsumer(o -> {
            log.info("connect> {}", o.getUtf8String());
        });
        waitingFor(Wait.forHttp("/connectors"));
    }

    public int getPort() {
        return KAFKA_CONNECT_INTERNAL_PORT;
    }

    public String getRegistryAddressInDockerNetwork() {
        return "http://"+kafkaConnectContainerName + ":" + KAFKA_CONNECT_INTERNAL_PORT;
    }

    public static KafkaConnectContainer create(String image, String boostrapServers, String zookeeperUrl) {
        return (KafkaConnectContainer) new KafkaConnectContainer(image, boostrapServers, zookeeperUrl)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                        .withName(kafkaConnectContainerName)
                );
    }
}
