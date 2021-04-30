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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import edu.umd.cs.findbugs.annotations.Nullable;

/** Session builder specialization that hooks in OpenSSL when that ssl provider is chosen. */
public class SessionBuilder extends CqlSessionBuilder {
  @Nullable private final SslConfig sslConfig;

  public SessionBuilder(@Nullable SslConfig sslConfig) {
    this.sslConfig = sslConfig;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    // CqlSessionBuilder.buildContext has some side-effects (adding dse type-codecs to typeCodecs)
    // that we also need.
    DriverContext baseContext = super.buildContext(configLoader, programmaticArguments);

    // If sslConfig is not provided we need to setup custom driver context for cloud
    if (sslConfig != null && sslConfig.getProvider() != SslConfig.Provider.OpenSSL) {
      // We're not using OpenSSL so the normal driver context is fine to use.
      return baseContext;
    }

    return new com.datastax.oss.common.sink.ssl.DriverContext(
        configLoader, programmaticArguments, sslConfig);
  }
}
