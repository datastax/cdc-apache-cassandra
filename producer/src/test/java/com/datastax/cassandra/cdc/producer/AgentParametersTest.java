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
package com.datastax.cassandra.cdc.producer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.datastax.cassandra.cdc.producer.ProducerConfig.*;

public class AgentParametersTest {

    String commonConfig =
            CDC_WORKING_DIR + "=cdc_working," +
                    ERROR_COMMITLOG_REPROCESS_ENABLED + "=true," +
                    CDC_DIR_POOL_INTERVAL_MS + "=1234," +
                    CDC_CONCURRENT_PROCESSOR + "=5," +
                    TOPIC_PREFIX + "=events-mutations," +
                    SSL_TRUSTSTORE_PATH + "=/truststore.jks," +
                    SSL_TRUSTSTORE_PASSWORD + "=password," +
                    SSL_TRUSTSTORE_TYPE + "=PKCS12," +
                    SSL_KEYSTORE_PATH + "=/keystore.jks," +
                    SSL_KEYSTORE_PASSWORD + "=password," +
                    SSL_ENABLED_PROTOCOLS + "=TLSv1.2," +
                    SSL_CIPHER_SUITES + "=AES256," +
                    SSL_PROVIDER + "=MyProvider,";

    void assertCommonConfig(ProducerConfig config) {
        assertEquals("cdc_working", config.cdcWorkingDir);
        assertEquals(true, config.errorCommitLogReprocessEnabled);
        assertEquals(1234L, config.cdcDirPollIntervalMs);
        assertEquals(5, config.cdcConcurrentProcessor);
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
                PULSAR_AUTH_PLUGIN_CLASS_NAME + "=MyAuthPlugin," +
                PULSAR_AUTH_PARAMS + "=x:y\\,z:t," +
                SSL_ALLOW_INSECURE_CONNECTION + "=true," +
                SSL_HOSTNAME_VERIFICATION_ENABLE + "=true,"
                ;

        ProducerConfig config = new ProducerConfig();
        config.configure(Platform.PULSAR, (String) null);     // test NPE
        config.configure(Platform.PULSAR, (Map<String, Object>) null);     // test NPE
        config.configure(Platform.PULSAR, agentArgs);
        assertCommonConfig(config);

        assertEquals("pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652", config.pulsarServiceUrl);

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

        ProducerConfig config = ProducerConfig.create(Platform.PULSAR, tenantInfo);
        assertEquals("pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652", config.pulsarServiceUrl);

        // Pulsar Auth
        assertEquals("MyAuthPlugin", config.pulsarAuthPluginClassName);
        assertEquals("sdds", config.pulsarAuthParams);
    }
}
