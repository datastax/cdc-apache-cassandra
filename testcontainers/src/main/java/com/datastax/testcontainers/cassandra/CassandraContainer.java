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
package com.datastax.testcontainers.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.testcontainers.cassandra.delegate.CassandraDatabaseDelegate;
import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.ext.ScriptUtils.ScriptLoadException;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import javax.script.ScriptException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

/**
 * Cassandra container
 * <p>
 * Supports 2.x and 3.x Cassandra versions
 *
 * @author Eugeny Karpov
 */
@Slf4j
public class CassandraContainer<SELF extends CassandraContainer<SELF>> extends GenericContainer<SELF> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("cassandra");
    private static final String DEFAULT_TAG = "3.11.2";

    @Deprecated
    public static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

    public static final Integer CQL_PORT = 9042;
    public static final String CONTAINER_CONFIG_LOCATION = "/etc/cassandra";
    public static final String USERNAME = "cassandra";
    public static final String PASSWORD = "cassandra";
    public static final String LOCAL_DC = "datacenter1";

    private String configLocation;
    private String initScriptPath;
    private Object metricRegistry;
    private String containerConfigLocation = CONTAINER_CONFIG_LOCATION;

    /**
     * @deprecated use {@link #CassandraContainer(DockerImageName)} instead
     */
    @Deprecated
    public CassandraContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public CassandraContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public CassandraContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        addExposedPort(CQL_PORT);
        addExposedPort(8000); // debug port
        addExposedPort(7199); // JMX port
        setStartupAttempts(1);
        withLogConsumer(o -> {
            log.info("[{}] {}", getContainerName(), o.getUtf8String().trim());
        });
    }

    @Override
    protected void configure() {
        optionallyMapResourceParameterAsVolume(containerConfigLocation, configLocation);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        runInitScriptIfRequired();
    }

    /**
     * Load init script content and apply it to the database if initScriptPath is set
     */
    private void runInitScriptIfRequired() {
        if (initScriptPath != null) {
            try {
                URL resource = Thread.currentThread().getContextClassLoader().getResource(initScriptPath);
                if (resource == null) {
                    logger().warn("Could not load classpath init script: {}", initScriptPath);
                    throw new ScriptLoadException("Could not load classpath init script: " + initScriptPath + ". Resource not found.");
                }
                String cql = IOUtils.toString(resource, StandardCharsets.UTF_8);
                DatabaseDelegate databaseDelegate = getDatabaseDelegate();
                ScriptUtils.executeDatabaseScript(databaseDelegate, initScriptPath, cql);
            } catch (IOException e) {
                logger().warn("Could not load classpath init script: {}", initScriptPath);
                throw new ScriptLoadException("Could not load classpath init script: " + initScriptPath, e);
            } catch (ScriptException e) {
                logger().error("Error while executing init script: {}", initScriptPath, e);
                throw new ScriptUtils.UncategorizedScriptException("Error while executing init script: " + initScriptPath, e);
            }
        }
    }

    /**
     * Map (effectively replace) directory in Docker with the content of resourceLocation if resource location is not null
     * <p>
     * Protected to allow for changing implementation by extending the class
     *
     * @param pathNameInContainer path in docker
     * @param resourceLocation    relative classpath to resource
     */
    protected void optionallyMapResourceParameterAsVolume(String pathNameInContainer, String resourceLocation) {
        Optional.ofNullable(resourceLocation)
                .map(MountableFile::forClasspathResource)
                .ifPresent(mountableFile -> withCopyFileToContainer(mountableFile, pathNameInContainer));
    }

    /**
     * Initialize Cassandra with the custom overridden Cassandra configuration
     * <p>
     * Be aware, that Docker effectively replaces all /etc/cassandra content with the content of config location, so if
     * Cassandra.yaml in configLocation is absent or corrupted, then Cassandra just won't launch
     *
     * @param configLocation relative classpath with the directory that contains cassandra.yaml and other configuration files
     */
    public SELF withConfigurationOverride(String configLocation) {
        this.configLocation = configLocation;
        return self();
    }

    public SELF withContainerConfigLocation(String containerConfigLocation) {
        this.containerConfigLocation = containerConfigLocation;
        return self();
    }

    /**
     * Initialize Cassandra with init CQL script
     * <p>
     * CQL script will be applied after container is started (see using WaitStrategy)
     *
     * @param initScriptPath relative classpath resource
     */
    public SELF withInitScript(String initScriptPath) {
        this.initScriptPath = initScriptPath;
        return self();
    }

    /**
     * Register an external Metric Registry object in the Cassandra driver,
     * see https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/metrics/#metric-registry
     *
     * @param metricRegistry
     */
    public SELF withMetricRegistry(Object metricRegistry) {
        this.metricRegistry = metricRegistry;
        return self();
    }

    /**
     * Get username
     * <p>
     * By default Cassandra has authenticator: AllowAllAuthenticator in cassandra.yaml
     * If username and password need to be used, then authenticator should be set as PasswordAuthenticator
     * (through custom Cassandra configuration) and through CQL with default cassandra-cassandra credentials
     * user management should be modified
     */
    public String getUsername() {
        return USERNAME;
    }

    /**
     * Get password
     * <p>
     * By default Cassandra has authenticator: AllowAllAuthenticator in cassandra.yaml
     * If username and password need to be used, then authenticator should be set as PasswordAuthenticator
     * (through custom Cassandra configuration) and through CQL with default cassandra-cassandra credentials
     * user management should be modified
     */
    public String getPassword() {
        return PASSWORD;
    }

    public String getCqlHostAddress() {
        return getHost() + ":" + getMappedPort(CassandraContainer.CQL_PORT);
    }

    public String getLocalDc() {
        return CassandraContainer.LOCAL_DC;
    }

    /**
     * Get configured Cluster
     * <p>
     * Can be used to obtain connections to Cassandra in the container
     */
    public CqlSession getCqlSession() {
        return getCqlSession(this, this.metricRegistry);
    }

    public static CqlSession getCqlSession(ContainerState containerState, Object meterRegistry) {
        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = new DefaultProgrammaticDriverConfigLoaderBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(60));

        InetSocketAddress endpoint = new InetSocketAddress(containerState.getHost(), containerState.getMappedPort(CQL_PORT));
        final CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(endpoint)
                .withConfigLoader(configLoaderBuilder.build())
                .withLocalDatacenter(LOCAL_DC);

        if (meterRegistry != null) {
            builder.withMetricRegistry(meterRegistry);
        }
        return builder.build();
    }

    public CqlSession getCqlSession(ContainerState containerState) {
        return getCqlSession(containerState, false);
    }

    private DatabaseDelegate getDatabaseDelegate() {
        return new CassandraDatabaseDelegate(this);
    }

    public static CassandraContainer<?> createCassandraContainerWithAgent(DockerImageName image,
                                                                          Network network,
                                                                          int nodeIndex,
                                                                          String cassandraVersion,
                                                                          String streamPlatform,
                                                                          String pulsarServiceUrl) {
        return createCassandraContainerWithAgent(image, network, nodeIndex, System.getProperty("buildDir"),
                String.format("agent-%s-%s", cassandraVersion, streamPlatform),
                String.format("pulsarServiceUrl=%s", pulsarServiceUrl),
                cassandraVersion);
    }

    public static CassandraContainer<?> createCassandraContainerWithAgent(DockerImageName image,
                                                                          Network network,
                                                                          int nodeIndex,
                                                                          String agentBuildDir,
                                                                          String agentName,
                                                                          String agentParams,
                                                                          String cassandraVersion) {
        String projectVersion = System.getProperty("projectVersion");
        String jarFile = String.format(Locale.ROOT, "%s-%s-all.jar", agentName, projectVersion);
        CassandraContainer<?> cassandraContainer = new CassandraContainer<>(image)
                .withCreateContainerCmdModifier(c -> c.withName("cassandra-" + nodeIndex))
                .withNetwork(network)
                .withConfigurationOverride("cassandra")
                .withFileSystemBind(
                        String.format(Locale.ROOT, "%s/libs/%s", agentBuildDir, jarFile),
                        String.format(Locale.ROOT, "/%s", jarFile))
                .withEnv("JVM_EXTRA_OPTS", String.format(Locale.ROOT,
                        "-javaagent:/%s=%s -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=" +
                        (cassandraVersion.equals("c4") ? "*:8000" : "8000"), jarFile, agentParams))
                .withEnv("MAX_HEAP_SIZE","1500m")
                .withEnv("HEAP_NEWSIZE","300m")
                .withEnv("DS_LICENSE", "accept")
                .withStartupTimeout(Duration.ofSeconds(150));
        if (nodeIndex > 1) {
            cassandraContainer.withEnv("CASSANDRA_SEEDS", "cassandra-1");   // for Cassandra
            cassandraContainer.withEnv("SEEDS", "cassandra-1");             // for DSE
        }
        return cassandraContainer;
    }
}
