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

package com.datastax.oss.cdc.backfill;

import picocli.CommandLine;

/**
 * Groups settings related to sending PK mutations to Pulsar's data topic.
 * TODO: Leverage arg groups/order
 */
public class ImportSettings {

    @CommandLine.Option(
            names = "--pulsar-url",
            description =
                    "The Pulsar broker service URL. ",
            defaultValue = "pulsar://localhost:6650")
    public String pulsarServiceUrl = "pulsar://localhost:6650";

    @CommandLine.Option(
            names = "--pulsar-auth-params",
            description =
                    "The Pulsar authentication parameters. ")
    public String pulsarAuthParams;

    @CommandLine.Option(
            names = "--pulsar-ssl-provider",
            description =
                    "The SSL/TLS provider to use. ")
    public String sslProvider;

    @CommandLine.Option(
            names = "--pulsar-ssl-truststore-path",
            description =
                    "The path to the SSL/TLS truststore file. ")
    public String sslTruststorePath;

    @CommandLine.Option(
            names = "--pulsar-ssl-truststore-password",
            description =
                    "The password for the SSL/TLS truststore. ")
    public String sslTruststorePassword;

    @CommandLine.Option(
            names = "--pulsar-ssl-truststore-type",
            description =
                    "The type of the SSL/TLS truststore.",
            defaultValue = "KJS")
    public String sslTruststoreType = "KJS";

    @CommandLine.Option(
            names = "--pulsar-ssl-keystore-path",
            description = "The path to the SSL/TLS keystore file.")
    public String sslKeystorePath;

    @CommandLine.Option(
            names = "--pulsar-ssl-keystore-password",
            description = "The password for the SSL/TLS keystore.")
    public String sslKeystorePassword;

    @CommandLine.Option(
            names = "--pulsar-ssl-cipher-suites",
            description = "Defines one or more cipher suites to use for negotiating the SSL/TLS connection.")
    public String sslCipherSuites;

    @CommandLine.Option(
            names = "--pulsar-ssl-enabled-protocols",
            description = "Enabled SSL/TLS protocols",
            defaultValue = "TLSv1.2,TLSv1.1,TLSv1")
    public String sslEnabledProtocols = "TLSv1.2,TLSv1.1,TLSv1";
    @CommandLine.Option(
            names = "--pulsar-ssl-allow-insecure-connections",
            description = "Allows insecure connections to servers whose certificate has not been signed by an approved CA. You should always disable `sslAllowInsecureConnection` in production environments.",
            defaultValue = "false")
    public boolean sslAllowInsecureConnection;

    @CommandLine.Option(
            names = "--pulsar-ssl-enable-hostname-verification",
            description = "Enable the server hostname verification.",
            defaultValue = "false")
    public boolean sslHostnameVerificationEnable;

    @CommandLine.Option(
            names = "--pulsar-ssl-tls-trust-certs-path",
            description = "The path to the trusted TLS certificate file.")
    public String tlsTrustCertsFilePath;

    @CommandLine.Option(
            names = "--pulsar-ssl-use-key-store-tls",
            description = "If TLS is enabled, specifies whether to use KeyStore type as TLS configuration parameter. ",
            defaultValue = "false")
    public boolean useKeyStoreTls;

    @CommandLine.Option(
            names = "--pulsar-auth-plugin-class-name",
            description = "The Pulsar authentication plugin class name.")
    public String pulsarAuthPluginClassName;

    @CommandLine.Option(
            names = "--events-topic-prefix",
            description = "The event topic name prefix. The `<keyspace_name>.<table_name>` is appended to that prefix to build the topic name.",
            defaultValue = "events-")
    public String topicPrefix = "events-";
}
