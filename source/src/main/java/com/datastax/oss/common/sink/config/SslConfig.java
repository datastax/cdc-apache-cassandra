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
package com.datastax.oss.common.sink.config;

import com.datastax.oss.common.sink.ConfigException;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.datastax.oss.common.sink.config.ConfigUtil.configToString;
import static com.datastax.oss.common.sink.config.ConfigUtil.getFilePath;

/** SSL configuration */
public class SslConfig extends AbstractConfig {
  public static final String PROVIDER_OPT = "ssl.provider";
  public static final String HOSTNAME_VALIDATION_OPT = "ssl.hostnameValidation";
  public static final String KEYSTORE_PASSWORD_OPT = "ssl.keystore.password";
  public static final String KEYSTORE_PATH_OPT = "ssl.keystore.path";
  public static final String OPENSSL_KEY_CERT_CHAIN_OPT = "ssl.openssl.keyCertChain";
  public static final String OPENSSL_PRIVATE_KEY_OPT = "ssl.openssl.privateKey";
  public static final String TRUSTSTORE_PASSWORD_OPT = "ssl.truststore.password";
  public static final String TRUSTSTORE_PATH_OPT = "ssl.truststore.path";
  static final String CIPHER_SUITES_OPT = "ssl.cipherSuites";

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              PROVIDER_OPT,
              ConfigDef.Type.STRING,
              "None",
              ConfigDef.Importance.HIGH,
              "None | JDK | OpenSSL")
          .define(
              CIPHER_SUITES_OPT,
              ConfigDef.Type.LIST,
              Collections.EMPTY_LIST,
              ConfigDef.Importance.HIGH,
              "The cipher suites to enable")
          .define(
              HOSTNAME_VALIDATION_OPT,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.HIGH,
              "Whether or not to validate node hostnames when using SSL")
          .define(
              KEYSTORE_PASSWORD_OPT,
              ConfigDef.Type.PASSWORD,
              "",
              ConfigDef.Importance.HIGH,
              "Keystore password")
          .define(
              KEYSTORE_PATH_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the keystore file")
          .define(
              OPENSSL_KEY_CERT_CHAIN_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the certificate chain file")
          .define(
              OPENSSL_PRIVATE_KEY_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the private key file")
          .define(
              TRUSTSTORE_PASSWORD_OPT,
              ConfigDef.Type.PASSWORD,
              "",
              ConfigDef.Importance.HIGH,
              "Truststore password")
          .define(
              TRUSTSTORE_PATH_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The path to the truststore file");

  private final @Nullable Path keystorePath;
  private final @Nullable Path truststorePath;
  private final @Nullable Path certFilePath;
  private final @Nullable Path privateKeyPath;
  private final @Nullable SslContext sslContext;

  public SslConfig(Map<String, String> sslSettings) {
    super(CONFIG_DEF, sslSettings, false);

    keystorePath = getFilePath(getString(KEYSTORE_PATH_OPT));
    truststorePath = getFilePath(getString(TRUSTSTORE_PATH_OPT));
    privateKeyPath = getFilePath(getString(OPENSSL_PRIVATE_KEY_OPT));
    certFilePath = getFilePath(getString(OPENSSL_KEY_CERT_CHAIN_OPT));

    ConfigUtil.assertAccessibleFile(keystorePath, KEYSTORE_PATH_OPT);
    ConfigUtil.assertAccessibleFile(truststorePath, TRUSTSTORE_PATH_OPT);
    ConfigUtil.assertAccessibleFile(privateKeyPath, OPENSSL_PRIVATE_KEY_OPT);
    ConfigUtil.assertAccessibleFile(certFilePath, OPENSSL_KEY_CERT_CHAIN_OPT);

    if (getProvider() == Provider.OpenSSL) {
      // Validate that either both or none of privateKeyPath and certFilePath are set.
      if ((certFilePath == null) != (privateKeyPath == null)) {
        throw new ConfigException(
            String.format(
                "%s cannot be set without %s and vice-versa: %s is not set",
                OPENSSL_KEY_CERT_CHAIN_OPT,
                OPENSSL_PRIVATE_KEY_OPT,
                certFilePath == null ? OPENSSL_KEY_CERT_CHAIN_OPT : OPENSSL_PRIVATE_KEY_OPT));
      }
      TrustManagerFactory tmf = null;
      String sslTrustStorePassword = getTruststorePassword();
      try {
        if (truststorePath != null) {
          KeyStore ks = KeyStore.getInstance("JKS");
          try {
            ks.load(
                new BufferedInputStream(new FileInputStream(truststorePath.toFile())),
                sslTrustStorePassword.toCharArray());
          } catch (IOException e) {
            if (e.getMessage() == null) {
              throw new ConfigException("Invalid truststore", e);
            } else {
              throw new ConfigException(String.format("Invalid truststore: %s", e.getMessage()), e);
            }
          }

          tmf = TrustManagerFactory.getInstance("SunX509");
          tmf.init(ks);
        }

        List<String> cipherSuites = getCipherSuites();

        SslContextBuilder builder =
            SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(tmf);
        if (!cipherSuites.isEmpty()) {
          builder.ciphers(cipherSuites);
        }
        if (certFilePath != null) {
          try {
            builder.keyManager(
                new BufferedInputStream(new FileInputStream(certFilePath.toFile())),
                new BufferedInputStream(new FileInputStream(privateKeyPath.toFile())));
          } catch (IllegalArgumentException e) {
            throw new ConfigException(
                String.format("Invalid certificate or private key: %s", e.getMessage()), e);
          }
        }
        this.sslContext = builder.build();
      } catch (GeneralSecurityException
          | FileNotFoundException
          | RuntimeException
          | SSLException e) {
        throw new ConfigException(
            String.format("Could not initialize OpenSSL context: %s", e.getMessage()), e);
      }
    } else {
      sslContext = null;
    }
  }

  public Provider getProvider() {
    String providerString = getString(PROVIDER_OPT);
    try {
      return Provider.valueOf(providerString);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          PROVIDER_OPT, providerString, "valid values are None, JDK, OpenSSL");
    }
  }

  public List<String> getCipherSuites() {
    return getList(CIPHER_SUITES_OPT);
  }

  public boolean requireHostnameValidation() {
    return getBoolean(HOSTNAME_VALIDATION_OPT);
  }

  @Nullable
  public Path getKeystorePath() {
    return keystorePath;
  }

  public String getKeystorePassword() {
    return getPassword(KEYSTORE_PASSWORD_OPT).value();
  }

  @Nullable
  public Path getTruststorePath() {
    return truststorePath;
  }

  public String getTruststorePassword() {
    return getPassword(TRUSTSTORE_PASSWORD_OPT).value();
  }

  @Nullable
  public SslContext getSslContext() {
    return sslContext;
  }

  @Override
  public String toString() {
    return configToString(
        this,
        "ssl.",
        PROVIDER_OPT,
        HOSTNAME_VALIDATION_OPT,
        KEYSTORE_PATH_OPT,
        KEYSTORE_PASSWORD_OPT,
        TRUSTSTORE_PATH_OPT,
        TRUSTSTORE_PASSWORD_OPT,
        OPENSSL_KEY_CERT_CHAIN_OPT,
        OPENSSL_PRIVATE_KEY_OPT);
  }

  Path getOpenSslKeyCertChain() {
    return certFilePath;
  }

  Path getOpenSslPrivateKey() {
    return privateKeyPath;
  }

  public enum Provider {
    None,
    JDK,
    OpenSSL
  }
}
