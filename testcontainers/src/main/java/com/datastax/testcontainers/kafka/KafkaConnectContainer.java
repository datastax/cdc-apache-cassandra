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

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

@Slf4j
public class KafkaConnectContainer<SELF extends KafkaConnectContainer<SELF>> extends GenericContainer<SELF> {

    public static final int KAFKA_CONNECT_INTERNAL_PORT = 8083;
    public static final String kafkaConnectContainerName = "connect";

    private KafkaConnectContainer(DockerImageName image, String boostrapServers, String schemaRegistryUrl) {
        super(image);

        addEnv("CONNECT_BOOTSTRAP_SERVERS", boostrapServers);
        addEnv("CONNECT_GROUP_ID", "connect");
        addEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect");
        addEnv("CONNECT_PRODUCER_COMPRESSION_TYPE", "lz4");
        addEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components,/usr/local/share/kafka/plugins/,/connect-plugins");
        addEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter");
        addEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryUrl);
        addEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter");
        addEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryUrl);
        addEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_config");
        addEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        addEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_offset");
        addEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        addEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_status");
        addEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        addEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        addEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        addEnv("CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        addEnv("CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        addEnv("CONNECT_LOG4J_LOGGERS", "org.apache.kafka.connect=DEBUG");

        withExposedPorts(KAFKA_CONNECT_INTERNAL_PORT);
        withLogConsumer(o -> {
            log.info("[{}] {}", getContainerName(), o.getUtf8String().trim());
        });
        waitingFor(Wait.forHttp("/connectors"));
    }

    public String getConnectUrl() {
        return "http://localhost:" + getMappedPort(KAFKA_CONNECT_INTERNAL_PORT);
    }

    public String getConnectUrlInDockerNetwork() {
        return "http://" + kafkaConnectContainerName + ":" + KAFKA_CONNECT_INTERNAL_PORT;
    }

    public static KafkaConnectContainer<?> create(DockerImageName image, String seed, String boostrapServers, String schemaRegistryUrl) {
        return (KafkaConnectContainer) new KafkaConnectContainer<>(image, boostrapServers, schemaRegistryUrl)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                        .withName(kafkaConnectContainerName + "-" + seed)
                );
    }

    /**
     * Post the content of the provided resource file to deploy a connector on Kafka Connect.
     * @param configResource
     * @return
     * @throws IOException
     */
    public int deployConnector(String configResource) throws IOException {
        URL u = new URL(getConnectUrl() + "/connectors");
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        OutputStream outputStream = conn.getOutputStream();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classloader.getResourceAsStream(configResource);
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);
        for (String line; (line = reader.readLine()) != null;) {
            outputStreamWriter.write(line);
        }
        outputStreamWriter.close();
        streamReader.close();
        return conn.getResponseCode();
    }

    /**
     * Undeploy a Kafka connect connector.
     * @param connectorName
     * @return
     * @throws IOException
     */
    public int undeployConnector(String connectorName) throws IOException {
        URL u = new URL(getConnectUrl() + "/connectors/" + connectorName);
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("DELETE");
        return conn.getResponseCode();
    }

    /**
     * Update the kafka connect logging level for a give classname.
     * @param className
     * @param level
     * @return
     * @throws IOException
     */
    public int setLogging(String className, String level) throws IOException {
        URL u = new URL(getConnectUrl() + "/admin/loggers/" + className);
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        OutputStream outputStream = conn.getOutputStream();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        outputStreamWriter.write(String.format(Locale.ROOT,"{\"level\": \"%s\"}", level));
        outputStreamWriter.close();
        return conn.getResponseCode();
    }
}
