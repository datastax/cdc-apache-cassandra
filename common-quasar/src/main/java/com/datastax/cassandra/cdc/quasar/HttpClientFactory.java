/*
 * Copyright (C) 2020 Strapdata SAS (support@strapdata.com)
 *
 * The Elassandra-Operator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The Elassandra-Operator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with the Elassandra-Operator.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.datastax.cassandra.cdc.quasar;

import io.micronaut.http.client.DefaultHttpClientConfiguration;
import io.micronaut.http.ssl.ClientSslConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.net.ssl.SSLException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

/**
 * This is a sidecar client factory that caches client and reuse it as possible.
 *
 * The periodic node status checker should invalidate cache entry that are not working.
 * Java DNS caching has been disabled. If a pod is restarted and its IP change,
 * the next nodeStatus check would invalidate the cache (calling invalidateClient()), and the next call to the factory would recreate the client.
 */
@Singleton
public class HttpClientFactory {

    static final Logger logger = LoggerFactory.getLogger(HttpClientFactory.class);

    ClientSslConfiguration clientSslConfiguration;
    ClientConfiguration quasarClientConfiguration;


    public HttpClientFactory(final ClientSslConfiguration clientSslConfiguration,
                             final ClientConfiguration quasarClientConfiguration) {
        this.clientSslConfiguration = clientSslConfiguration;
        this.quasarClientConfiguration = quasarClientConfiguration;
    }

    /**
     * Get a sidecar client from cache or create it
     */
    public synchronized HttpClient clientForOrdinal(final int ordinal, final boolean withSsl) throws MalformedURLException, InterruptedException, ExecutionException, SSLException {
        URL url = withSsl
                ? new URL("https://" + quasarClientConfiguration.getServiceName() + "-" + ordinal + ":" + quasarClientConfiguration.getPort())
                : new URL("http://" + quasarClientConfiguration.getServiceName() + "-" + ordinal + ":" + quasarClientConfiguration.getPort());
        logger.debug("creating HTTP client for ordinal={} url={}", ordinal, url.toString());
        DefaultHttpClientConfiguration httpClientConfiguration = new DefaultHttpClientConfiguration();
        httpClientConfiguration.setReadTimeout(Duration.ofSeconds(30));
        HttpClient client = new HttpClient(url, httpClientConfiguration);
        return client;
    }

    public void invalidateClient(int ordinal, Throwable throwable) {
        logger.debug("invalidating cached client for ordinal="+ordinal, throwable);
    }
}
