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
package com.datastax.oss.cdc;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.auth.DseGssApiAuthProvider;
import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.config.ContactPointsValidator;
import com.datastax.oss.common.sink.config.SslConfig;
import com.datastax.oss.common.sink.ssl.SessionBuilder;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.AUTH_PROVIDER_SERVICE;
import static com.datastax.oss.common.sink.util.UUIDUtil.generateClientId;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.*;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

/**
 * Async read from Cassandra with downgrade consistency retry.
 */
@Slf4j
@Getter
@SuppressWarnings("try")
public class CassandraClient implements AutoCloseable {

    final CqlSession cqlSession;

    public CassandraClient(CassandraSourceConnectorConfig config, String version, String applicationName, SchemaChangeListener schemaChangeListener) {
        this.cqlSession = buildCqlSession(config, version, applicationName, schemaChangeListener);
    }

    public static CqlSession buildCqlSession(
            CassandraSourceConnectorConfig config,
            String version, String applicationName,
            SchemaChangeListener schemaChangeListener) {
        log.info("CassandraClient starting with config:\n{}\n", config.toString());
        SslConfig sslConfig = config.getSslConfig();

        // refresh only our keyspace.
        OptionsMap optionsMap = OptionsMap.driverDefaults();
        optionsMap.put(TypedDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Arrays.asList(config.getKeyspaceName()));
        DriverConfigLoader loader = DriverConfigLoader.fromMap(optionsMap);

        CqlSessionBuilder builder =
                new SessionBuilder(sslConfig)
                        .withConfigLoader(loader)
                        .withApplicationVersion(version)
                        .withApplicationName(applicationName)
                        .withClientId(generateClientId(config.getInstanceName()))
                        .withKeyspace(config.getKeyspaceName())
                        .withSchemaChangeListener(schemaChangeListener);

        ContactPointsValidator.validateContactPoints(config.getContactPoints());

        if (sslConfig != null && sslConfig.requireHostnameValidation()) {
            // if requireHostnameValidation then InetSocketAddress must be resolved
            config
                    .getContactPoints()
                    .stream()
                    .map(hostStr -> new InetSocketAddress(hostStr, config.getPort()))
                    .forEach(builder::addContactPoint);
        } else {
            config
                    .getContactPoints()
                    .stream()
                    .map(hostStr -> InetSocketAddress.createUnresolved(hostStr, config.getPort()))
                    .forEach(builder::addContactPoint);
        }

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
                dseProgrammaticBuilderWithFallback(
                        ConfigFactory.parseMap(config.getJavaDriverSettings(), "Connector properties"));

        processAuthenticatorConfig(config, configLoaderBuilder);
        if (sslConfig != null) {
            processSslConfig(sslConfig, configLoaderBuilder);
        }
        builder.withConfigLoader(configLoaderBuilder.build());

        CqlSession cqlSession = builder.build();
        cqlSession.setSchemaMetadataEnabled(true);
        return cqlSession;
    }

    public CqlIdentifier[] buildProjectionClause(List<ColumnMetadata> columns) {
        CqlIdentifier[] projection = new CqlIdentifier[columns.size()];
        int i = 0;
        for (ColumnMetadata column : columns) {
            projection[i++] = column.getName();
        }
        return projection;
    }

    public CqlIdentifier[] buildPrimaryKeyClause(TableMetadata tableMetadata) {
        CqlIdentifier[] pkClause = new CqlIdentifier[tableMetadata.getPrimaryKey().size()];
        int i = 0;
        for (ColumnMetadata column : tableMetadata.getPrimaryKey()) {
            pkClause[i++] = column.getName();
        }
        return pkClause;
    }

    public PreparedStatement prepareSelect(String keyspaceName, String tableName,
                                CqlIdentifier[] projection,
                                CqlIdentifier[] pkClause,
                                int pkLength) {
        Select query = selectFrom(keyspaceName, tableName).columns(projection);
        for(int i = 0; i < pkLength; i++)
            query = query.whereColumn(pkClause[i]).isEqualTo(bindMarker());
        log.info(query.asCql());
        return cqlSession.prepare(query.asCql());
    }

    /**
     * Process ssl settings in the config; essentially map them to settings in the session builder.
     *
     * @param sslConfig           the ssl config
     * @param configLoaderBuilder the config loader builder
     */
    private static void processSslConfig(
            SslConfig sslConfig, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder) {
        if (sslConfig.getProvider() == SslConfig.Provider.JDK) {
            configLoaderBuilder.withString(SSL_ENGINE_FACTORY_CLASS, "DefaultSslEngineFactory");
            List<String> cipherSuites = sslConfig.getCipherSuites();
            if (!cipherSuites.isEmpty()) {
                configLoaderBuilder.withStringList(SSL_CIPHER_SUITES, cipherSuites);
            }
            configLoaderBuilder
                    .withBoolean(SSL_HOSTNAME_VALIDATION, sslConfig.requireHostnameValidation())
                    .withString(SSL_TRUSTSTORE_PASSWORD, sslConfig.getTruststorePassword())
                    .withString(SSL_KEYSTORE_PASSWORD, sslConfig.getKeystorePassword());

            Path truststorePath = sslConfig.getTruststorePath();
            if (truststorePath != null) {
                configLoaderBuilder.withString(SSL_TRUSTSTORE_PATH, truststorePath.toString());
            }
            Path keystorePath = sslConfig.getKeystorePath();
            if (keystorePath != null) {
                configLoaderBuilder.withString(SSL_KEYSTORE_PATH, keystorePath.toString());
            }
        }
    }

    /**
     * Process auth settings in the config; essentially map them to settings in the session builder.
     *
     * @param config              the sink config
     * @param configLoaderBuilder the config loader builder
     */
    private static void processAuthenticatorConfig(
            CassandraSourceConnectorConfig config, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder) {
        AuthenticatorConfig authConfig = config.getAuthenticatorConfig();
        if (authConfig.getProvider() == AuthenticatorConfig.Provider.PLAIN) {
            configLoaderBuilder
                    .withClass(AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                    .withString(AUTH_PROVIDER_USER_NAME, authConfig.getUsername())
                    .withString(AUTH_PROVIDER_PASSWORD, authConfig.getPassword());
        } else if (authConfig.getProvider() == AuthenticatorConfig.Provider.GSSAPI) {
            Path keyTabPath = authConfig.getKeyTabPath();
            Map<String, String> loginConfig;
            if (keyTabPath == null) {
                // Rely on the ticket cache.
                ImmutableMap.Builder<String, String> loginConfigBuilder =
                        ImmutableMap.<String, String>builder()
                                .put("useTicketCache", "true")
                                .put("refreshKrb5Config", "true")
                                .put("renewTGT", "true");
                if (!authConfig.getPrincipal().isEmpty()) {
                    loginConfigBuilder.put("principal", authConfig.getPrincipal());
                }
                loginConfig = loginConfigBuilder.build();
            } else {
                // Authenticate with the keytab file
                loginConfig =
                        ImmutableMap.of(
                                "principal",
                                authConfig.getPrincipal(),
                                "useKeyTab",
                                "true",
                                "refreshKrb5Config",
                                "true",
                                "keyTab",
                                authConfig.getKeyTabPath().toString());
            }
            configLoaderBuilder
                    .withClass(AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
                    .withString(AUTH_PROVIDER_SERVICE, authConfig.getService())
                    .withStringMap(
                            AUTH_PROVIDER_SASL_PROPERTIES, ImmutableMap.of("javax.security.sasl.qop", "auth"))
                    .withStringMap(DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION, loginConfig);
        }
    }

    @NonNull
    private static ProgrammaticDriverConfigLoaderBuilder dseProgrammaticBuilderWithFallback(
            Config properties) {
        ConfigFactory.invalidateCaches();
        return new DefaultProgrammaticDriverConfigLoaderBuilder(
                () ->
                        ConfigFactory.defaultApplication()
                                .withFallback(properties)
                                .withFallback(ConfigFactory.parseResourcesAnySyntax("dse-reference"))
                                .withFallback(ConfigFactory.defaultReference()),
                DefaultDriverConfigLoader.DEFAULT_ROOT_PATH);
    }

    @Override
    public void close() throws Exception {
        this.cqlSession.close();
    }


    public Tuple2<KeyspaceMetadata, TableMetadata> getTableMetadata(String keyspace, String table) {
        Metadata metadata = cqlSession.getMetadata();
        Optional<KeyspaceMetadata> keyspaceMetadataOptional = metadata.getKeyspace(keyspace);
        if (!keyspaceMetadataOptional.isPresent()) {
            throw new IllegalArgumentException("No metadata for keyspace " + keyspace);
        }
        Optional<TableMetadata> tableMetadataOptional = keyspaceMetadataOptional.get().getTable(table);
        if (!tableMetadataOptional.isPresent()) {
            throw new IllegalArgumentException("No metadata for table " + keyspace + "." + table);
        }
        return new Tuple2<>(keyspaceMetadataOptional.get(), tableMetadataOptional.get());
    }

    public Tuple3<Row, ConsistencyLevel, UUID> selectRow(List<Object> pkValues,
                                                         UUID nodeId,
                                                         List<ConsistencyLevel> consistencyLevels,
                                                         PreparedStatement preparedStatement,
                                                         String md5Digest)
            throws ExecutionException, InterruptedException {
        return selectRowAsync(pkValues, nodeId, consistencyLevels, preparedStatement, md5Digest)
                .toCompletableFuture().get();
    }

    /**
     * Try to read with downgraded consistency
     * @param pkValues primary key column
     * @param nodeId coordinator node id
     * @param consistencyLevels list of consistency to retry
     * @param preparedStatement CQL prepared statement
     * @param md5Digest mutation MD5 digest
     */
    public CompletionStage<Tuple3<Row, ConsistencyLevel, UUID>> selectRowAsync(List<Object> pkValues,
                                                                               UUID nodeId,
                                                                               List<ConsistencyLevel> consistencyLevels,
                                                                               PreparedStatement preparedStatement,
                                                                               String md5Digest) {
        BoundStatement statement = preparedStatement.bind(pkValues.toArray(new Object[pkValues.size()]));

        // set the coordinator node
        Node node = null;
        if (nodeId != null) {
            node = cqlSession.getMetadata().getNodes().get(nodeId);
            if (node != null) {
                statement = statement.setNode(node);
            }
        }
        log.info("Fetching md5Digest={} coordinator={} query={} pk={} ", md5Digest, node, preparedStatement.getQuery(), pkValues);

        return executeWithDowngradeConsistencyRetry(cqlSession, statement, consistencyLevels)
                .thenApply(tuple -> {
                    log.info("Read cl={} coordinator={} pk={}", tuple._2, tuple._1.getExecutionInfo().getCoordinator().getHostId(), pkValues);
                    Row row = tuple._1.one();
                    return new Tuple3<>(row, tuple._2, tuple._1.getExecutionInfo().getCoordinator().getHostId());
                })
                .whenComplete((tuple, error) -> {
                    if (error != null) {
                        log.warn("Failed to retrieve row: {}", error);
                    }
                });
    }

    CompletionStage<Tuple2<AsyncResultSet, ConsistencyLevel>> executeWithDowngradeConsistencyRetry(
            CqlSession cqlSession,
            BoundStatement statement,
            List<ConsistencyLevel> consistencyLevels) {
        final ConsistencyLevel cl = consistencyLevels.remove(0);
        statement.setConsistencyLevel(cl);
        log.info("Trying with CL={} statement={}", cl, statement);
        final CompletionStage<Tuple2<AsyncResultSet, ConsistencyLevel>> completionStage =
                cqlSession.executeAsync(statement).thenApply(rx -> new Tuple2<>(rx, cl));
        return completionStage
                .handle((r, ex) -> {
                    if (ex == null || !(ex instanceof UnavailableException) || consistencyLevels.isEmpty()) {
                        log.error("Executed CL={} statement={}", cl, statement);
                        return completionStage;
                    }
                    return completionStage
                            .handleAsync((r1, ex1) ->
                                    executeWithDowngradeConsistencyRetry(cqlSession, statement, consistencyLevels))
                            .thenCompose(Function.identity());
                })
                .thenCompose(Function.identity());
    }
}
