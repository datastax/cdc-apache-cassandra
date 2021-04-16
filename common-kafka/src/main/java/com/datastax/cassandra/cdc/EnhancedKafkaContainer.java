package com.datastax.cassandra.cdc;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class EnhancedKafkaContainer extends KafkaContainer {

    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.1";

    static final String KAFKA_IMAGE = "confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION;

    public static final String kafkaContainerName = "kafka";
    public static final int KAFKA_INTERNAL_PORT = 9093;

    private EnhancedKafkaContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public int getPort() {
        return KAFKA_INTERNAL_PORT;
    }

    @Override
    public String getBootstrapServers() {
        // we have to override this function
        // because we want the Kafka Broker to advertise itself
        // with the docker network address
        // otherwise the Kafka Schema Registry won't work
        return "PLAINTEXT://" + kafkaContainerName + ":" + KAFKA_INTERNAL_PORT;
    }

    public String getBootstrapServersHostPort() {
        return kafkaContainerName + ":" + KAFKA_INTERNAL_PORT;
    }

    public String getZooKeeperAddressInDockerNetwork() {
        return kafkaContainerName +":2181";
    }

    public static EnhancedKafkaContainer create() {
        return (EnhancedKafkaContainer) new EnhancedKafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withEmbeddedZookeeper()
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                        .withName(kafkaContainerName)
                );
    }

}
