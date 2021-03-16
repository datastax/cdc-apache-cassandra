package com.datastax.cassandra.cdc.quasar;


import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.runtime.ApplicationConfiguration;

import java.time.Duration;


@ConfigurationProperties(ClientConfiguration.PREFIX)
@Requires(property = ClientConfiguration.PREFIX)
public class ClientConfiguration extends HttpClientConfiguration {

    public static final String PREFIX = "quasarcli";

    /**
     * HTTP client connection pool configuration.
     */
    private final QuasarClientConnectionPoolConfiguration connectionPoolConfiguration;

    /**
     * Quasar DNS serviceNameTemplate.
     */
    String serviceName;

    /**
     * Quasar node port.
     */
    Integer port = 8081;

    public ClientConfiguration(
            final ApplicationConfiguration applicationConfiguration,
            final QuasarClientConnectionPoolConfiguration connectionPoolConfiguration) {
        super(applicationConfiguration);
        this.connectionPoolConfiguration = connectionPoolConfiguration;
    }

    /**
     * Obtains the connection pool configuration.
     *
     * @return The connection pool configuration.
     */
    @Override
    public ConnectionPoolConfiguration getConnectionPoolConfiguration() {
        return connectionPoolConfiguration;
    }

    public String getServiceName() {
        return this.serviceName;
    }

    public Integer getPort() {
        return this.port;
    }

    @ConfigurationProperties(ConnectionPoolConfiguration.PREFIX)
    public static class QuasarClientConnectionPoolConfiguration extends ConnectionPoolConfiguration {
    }

    /**
     * Extra configuration propertie to set the values
     * for the @Retryable annotation on the WeatherClient.
     */
    @ConfigurationProperties(QuasarClientRetryConfiguration.PREFIX)
    public static class QuasarClientRetryConfiguration {

        public static final String PREFIX = "retry";

        private Duration delay;

        private int attempts;

        public Duration getDelay() {
            return delay;
        }

        public void setDelay(final Duration delay) {
            this.delay = delay;
        }

        public int getAttempts() {
            return attempts;
        }

        public void setAttempts(final int attempts) {
            this.attempts = attempts;
        }
    }
}
