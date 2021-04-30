/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.common.sink.ssl;

import com.datastax.oss.common.sink.config.SslConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Optional;

/**
 * Specialization of DefaultDriverContext that allows the connector to use OpenSSL or SniSslEngine.
 */
public class DriverContext extends DefaultDriverContext {
  @Nullable private final SslConfig sslConfig;

  DriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      @Nullable SslConfig sslConfig) {
    super(configLoader, programmaticArguments);
    this.sslConfig = sslConfig;
  }

  @Override
  protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
    // If a JDK-based factory was provided through the public API, wrap it;
    // this can only happen in kafka-connector if a secure connect bundle was provided.
    if (getSslEngineFactory().isPresent()) {
      return getSslEngineFactory().map(JdkSslHandlerFactory::new);
    } else if (sslConfig != null) {
      return Optional.of(
          new OpenSslHandlerFactory(
              sslConfig.getSslContext(), sslConfig.requireHostnameValidation()));
    } else {
      throw new IllegalStateException(
          "Neither sslConfig nor secure bundle was provided to configure SslHandlerFactory");
    }
  }
}
