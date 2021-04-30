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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/** Factory for creating OpenSSL ssl handlers. */
public class OpenSslHandlerFactory implements SslHandlerFactory {
  private final SslContext context;
  private final boolean requireHostValidation;

  OpenSslHandlerFactory(SslContext context, boolean requireHostValidation) {
    this.context = context;
    this.requireHostValidation = requireHostValidation;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
    SslHandler sslHandler;
    SocketAddress socketAddress = remoteEndpoint.resolve();
    if (socketAddress instanceof InetSocketAddress) {
      InetSocketAddress inetAddr = (InetSocketAddress) socketAddress;
      sslHandler = context.newHandler(channel.alloc(), inetAddr.getHostName(), inetAddr.getPort());
    } else {
      sslHandler = context.newHandler(channel.alloc());
    }

    if (requireHostValidation) {
      SSLEngine sslEngine = sslHandler.engine();
      SSLParameters sslParameters = sslEngine.getSSLParameters();
      sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
      sslEngine.setSSLParameters(sslParameters);
    }
    return sslHandler;
  }

  @Override
  public void close() {
    // No-op
  }
}
