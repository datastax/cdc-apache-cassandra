/**
 * Copyright DataStax, Inc 2021.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.cdc.producer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.datastax.cassandra.cdc.producer.ProducerConfig.*;

public class AgentParametersTest {

    @Test
    public void testConfigure() {
        String agentArgs =
                CDC_RELOCATION_DIR + "=cdc_mybackup," +
                        ERROR_COMMITLOG_REPROCESS_ENABLED + "=true," +
                        CDC_DIR_POOL_INTERVAL_MS + "=1234," +
                        TOPIC_PREFIX + "=events-mutations," +
                        PULSAR_SERVICE_URL + "=pulsar+ssl://mypulsar:6650\\,localhost:6651\\,localhost:6652," +
                        KAFKA_BROKERS + "=mykafka:9092," +
                        KAFKA_SCHEMA_REGISTRY_URL + "=http://myregistry:8081," +
                        SSL_TRUSTSTORE_PATH + "=/truststore.jks," +
                        SSL_TRUSTSTORE_PASSWORD + "=password," +
                        SSL_TRUSTSTORE_TYPE + "=PKCS12," +
                        SSL_KEYSTORE_PATH + "=/keystore.jks," +
                        SSL_KEYSTORE_PASSWORD + "=password," +
                        SSL_ALLOW_INSECURE_CONNECTION + "=true," +
                        SSL_HOSTNAME_VERIFICATION_ENABLE + "=true," +
                        SSL_ENABLED_PROTOCOLS + "=TLSv1.2," +
                        SSL_CIPHER_SUITES + "=AES256," +
                        SSL_ENDPOINT_IDENTIFICATION_ALGORITHM + "=none," +
                        SSL_PROVIDER + "=MyProvider," +
                        PULSAR_AUTH_PLUGIN_CLASS_NAME + "=MyAuthPlugin," +
                        PULSAR_AUTH_PARAMS + "=x:y\\,z:t," +
                        KAFKA_PROPERTIES + "=security.protocol=SASL_SSL\\,sasl.mechanism=PLAIN"
                ;
        ProducerConfig.configure(null);     // test NPE
        ProducerConfig.configure(agentArgs);
        assertEquals("cdc_mybackup", cdcRelocationDir);
        assertEquals( true, errorCommitLogReprocessEnabled);
        assertEquals(1234L, cdcDirPollIntervalMs);
        assertEquals( "events-mutations", topicPrefix);
        assertEquals( "pulsar+ssl://mypulsar:6650,localhost:6651,localhost:6652", pulsarServiceUrl);
        assertEquals( "mykafka:9092", kafkaBrokers);
        assertEquals( "http://myregistry:8081", kafkaSchemaRegistryUrl);

        // common TLS settings
        assertEquals( "/truststore.jks", sslTruststorePath);
        assertEquals("password", sslTruststorePassword);
        assertEquals( "PKCS12", sslTruststoreType);
        assertEquals( "/keystore.jks", sslKeystorePath);
        assertEquals("password", sslKeystorePassword);
        assertEquals( true, sslAllowInsecureConnection);
        assertEquals( true, sslHostnameVerificationEnable);
        assertEquals( "TLSv1.2", sslEnabledProtocols);
        assertEquals("AES256", sslCipherSuites);
        assertEquals("MyProvider", sslProvider);

        // Kafka custom settings
        assertEquals("SASL_SSL", kafkaProperties.get("security.protocol"));
        assertEquals("PLAIN", kafkaProperties.get("sasl.mechanism"));

        // Pulsar Auth
        assertEquals("MyAuthPlugin", pulsarAuthPluginClassName);
        assertEquals("x:y,z:t", pulsarAuthParams);
    }
}
