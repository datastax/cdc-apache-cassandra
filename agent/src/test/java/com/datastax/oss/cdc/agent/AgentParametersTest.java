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
package com.datastax.oss.cdc.agent;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetSystemProperty;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.datastax.oss.cdc.agent.AgentConfig.*;

public class AgentParametersTest {

    String commonConfig =
            CDC_WORKING_DIR + "=cdc_working," +
                    ERROR_COMMITLOG_REPROCESS_ENABLED + "=true," +
                    CDC_DIR_POLL_INTERVAL_MS + "=1234," +
                    CDC_CONCURRENT_PROCESSORS + "=5," +
                    MAX_INFLIGHT_MESSAGES_PER_TASK + "=50," +
                    TOPIC_PREFIX + "=events-mutations," +
                    SSL_TRUSTSTORE_PATH + "=/truststore.jks," +
                    SSL_TRUSTSTORE_PASSWORD + "=password," +
                    SSL_TRUSTSTORE_TYPE + "=PKCS12," +
                    SSL_KEYSTORE_PATH + "=/keystore.jks," +
                    SSL_KEYSTORE_PASSWORD + "=password," +
                    SSL_ENABLED_PROTOCOLS + "=TLSv1.2," +
                    SSL_CIPHER_SUITES + "=AES256," +
                    SSL_PROVIDER + "=MyProvider,";

    void assertCommonConfig(AgentConfig config) {
        assertEquals("cdc_working", config.cdcWorkingDir);
        assertEquals(true, config.errorCommitLogReprocessEnabled);
        assertEquals(1234L, config.cdcDirPollIntervalMs);
        assertEquals(5, config.cdcConcurrentProcessors);
        assertEquals(50, config.maxInflightMessagesPerTask);
        assertEquals("events-mutations", config.topicPrefix);

        // common TLS settings
        assertEquals("/truststore.jks", config.sslTruststorePath);
        assertEquals("password", config.sslTruststorePassword);
        assertEquals("PKCS12", config.sslTruststoreType);
        assertEquals("/keystore.jks", config.sslKeystorePath);
        assertEquals("password", config.sslKeystorePassword);
        assertEquals("TLSv1.2", config.sslEnabledProtocols);
        assertEquals("AES256", config.sslCipherSuites);
        assertEquals("MyProvider", config.sslProvider);
    }

    @Test
    public void testConfigurePulsar() {
        String agentArgs = commonConfig +
                PULSAR_SERVICE_URL + "=pulsar+ssl://mypulsar:6650\\,localhost:6651\\,localhost:6652," +
                PULSAR_BATCH_DELAY_IN_MS + "=20," +
                PULSAR_KEY_BASED_BATCHER + "=true," +
                PULSAR_MAX_PENDING_MESSAGES + "=20," +
                PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS + "=200," +
                PULSAR_AUTH_PLUGIN_CLASS_NAME + "=MyAuthPlugin," +
                PULSAR_AUTH_PARAMS + "=x:y\\,z:t," +
                SSL_ALLOW_INSECURE_CONNECTION + "=true," +
                SSL_HOSTNAME_VERIFICATION_ENABLE + "=true,"
                ;

        AgentConfig config = new AgentConfig();
        config.configure(Platform.PULSAR, (String) null);     // test NPE
        config.configure(Platform.PULSAR, (Map<String, Object>) null);     // test NPE
        config.configure(Platform.PULSAR, agentArgs);
        assertCommonConfig(config);

        assertEquals("pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652", config.pulsarServiceUrl);
        assertEquals(20L, config.pulsarBatchDelayInMs);
        assertTrue(config.pulsarKeyBasedBatcher);
        assertEquals(20, config.pulsarMaxPendingMessages);
        assertEquals(200, config.pulsarMaxPendingMessagesAcrossPartitions);

        // Pulsar Auth
        assertEquals("MyAuthPlugin", config.pulsarAuthPluginClassName);
        assertEquals("x:y,z:t", config.pulsarAuthParams);
    }

    @Test
    public void testConfigurePulsarFromMap() {
        Map<String, Object> tenantInfo = new HashMap<>();
        tenantInfo.put(PULSAR_SERVICE_URL, "pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652");
        tenantInfo.put(PULSAR_AUTH_PLUGIN_CLASS_NAME, "MyAuthPlugin");
        tenantInfo.put(PULSAR_AUTH_PARAMS, "sdds");
        tenantInfo.put(SSL_ALLOW_INSECURE_CONNECTION, "true");
        tenantInfo.put(SSL_HOSTNAME_VERIFICATION_ENABLE, "true");

        AgentConfig config = AgentConfig.create(Platform.PULSAR, tenantInfo);
        assertEquals("pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652", config.pulsarServiceUrl);

        // Pulsar Auth
        assertEquals("MyAuthPlugin", config.pulsarAuthPluginClassName);
        assertEquals("sdds", config.pulsarAuthParams);
    }

    @Test
    @SetEnvironmentVariable(key = "CDC_PULSAR_SERVICE_URL", value = "pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652")
    @SetEnvironmentVariable(key = "CDC_PULSAR_AUTH_PLUGIN_CLASS_NAME", value = "MyAuthPlugin")
    @SetEnvironmentVariable(key = "CDC_PULSAR_AUTH_PARAMS", value = "sdds")
    @SetEnvironmentVariable(key = "CDC_SSL_ALLOW_INSECURE_CONNECTION", value = "true")
    @SetEnvironmentVariable(key = "CDC_SSL_HOSTNAME_VERIFICATION_ENABLE", value = "true")
    @SetEnvironmentVariable(key = "CDC_PULSAR_BATCH_DELAY_IN_MS", value = "555")
    @SetEnvironmentVariable(key = "CDC_PULSAR_KEY_BASED_BATCHER", value = "true")
    @SetEnvironmentVariable(key = "CDC_PULSAR_MAX_PENDING_MESSAGES", value = "555")
    public void testConfigurePulsarFromEnvVar() {
        AgentConfig config = AgentConfig.create(Platform.PULSAR, "");
        assertEquals("pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652", config.pulsarServiceUrl);

        assertEquals(true, config.sslAllowInsecureConnection);
        assertEquals(true, config.sslHostnameVerificationEnable);

        assertEquals(555L, config.pulsarBatchDelayInMs);
        assertEquals(true, config.pulsarKeyBasedBatcher);
        assertEquals(555, config.pulsarMaxPendingMessages);

        // Pulsar Auth
        assertEquals("MyAuthPlugin", config.pulsarAuthPluginClassName);
        assertEquals("sdds", config.pulsarAuthParams);
    }

    @Test
    @SetEnvironmentVariable(key = "CDC_SSL_TRUSTSTORE_PATH", value = "/truststore.jks")
    @SetEnvironmentVariable(key = "CDC_SSL_TRUSTSTORE_PASSWORD", value = "password1")
    @SetEnvironmentVariable(key = "CDC_SSL_TRUSTSTORE_TYPE", value = "PKCS12")
    @SetEnvironmentVariable(key = "CDC_SSL_KEYSTORE_PATH", value = "/keystore.jks")
    @SetEnvironmentVariable(key = "CDC_SSL_KEYSTORE_PASSWORD", value = "password2")
    @SetEnvironmentVariable(key = "CDC_SSL_ENABLED_PROTOCOLS", value = "TLSv1.2")
    @SetEnvironmentVariable(key = "CDC_SSL_CIPHER_SUITES", value = "AES256")
    @SetEnvironmentVariable(key = "CDC_SSL_PROVIDER", value = "MyProvider")
    public void testConfigureSslFromEnvVar() {
        AgentConfig config = AgentConfig.create(Platform.PULSAR, "");
        assertEquals("/truststore.jks", config.sslTruststorePath);
        assertEquals("password1", config.sslTruststorePassword);
        assertEquals("PKCS12", config.sslTruststoreType);
        assertEquals("/keystore.jks", config.sslKeystorePath);
        assertEquals("password2", config.sslKeystorePassword);
        assertEquals("TLSv1.2", config.sslEnabledProtocols);
        assertEquals("AES256", config.sslCipherSuites);
        assertEquals("MyProvider", config.sslProvider);
    }

    @Test
    @SetSystemProperty(key = "cassandra.storagedir", value = "toto")
    @SetEnvironmentVariable(key = "CDC_WORKING_DIR", value = "toto/cdc2")
    @SetEnvironmentVariable(key = "CDC_DIR_POLL_INTERVAL_MS", value = "555")
    @SetEnvironmentVariable(key = "CDC_CONCURRENT_PROCESSORS", value = "16")
    @SetEnvironmentVariable(key = "CDC_ERROR_COMMITLOG_REPROCESS_ENABLED", value = "true")
    @SetEnvironmentVariable(key = "CDC_TOPIC_PREFIX", value = "myevents-")
    @SetEnvironmentVariable(key = "CDC_MAX_INFLIGHT_MESSAGES_PER_TASK", value = "55")
    public void testConfigureCdcFromEnvVar() {
        AgentConfig config = AgentConfig.create(Platform.PULSAR, "");
        assertEquals("toto/cdc2", config.cdcWorkingDir);
        assertEquals(555, config.cdcDirPollIntervalMs);
        assertEquals(16, config.cdcConcurrentProcessors);
        assertEquals(true, config.errorCommitLogReprocessEnabled);
        assertEquals(55, config.maxInflightMessagesPerTask);
    }

    @Test
    @SetSystemProperty(key = "cassandra.storagedir", value = "toto")
    public void testConfigureCdcWorkingDir() {
        AgentConfig config = AgentConfig.create(Platform.PULSAR, "");
        assertEquals("toto/cdc", config.cdcWorkingDir);
    }
}
