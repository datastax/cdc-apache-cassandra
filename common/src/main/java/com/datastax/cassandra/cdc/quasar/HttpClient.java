package com.datastax.cassandra.cdc.quasar;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.annotation.AnnotationMetadataResolver;
import io.micronaut.core.io.ResourceResolver;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.LoadBalancer;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.micronaut.http.client.netty.ssl.NettyClientSslBuilder;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.micronaut.jackson.ObjectMapperFactory;
import io.micronaut.jackson.codec.JsonMediaTypeCodec;
import io.micronaut.jackson.codec.JsonStreamMediaTypeCodec;
import io.micronaut.runtime.ApplicationConfiguration;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Locale;
import java.util.UUID;

import static io.micronaut.http.HttpRequest.GET;
import static io.micronaut.http.HttpRequest.POST;

/**
 * Currently @Client annotation advice that generates the client code from an interface is totally static and cannot
 * be used to configure client with dynamic urls. See {@link io.micronaut.http.client.interceptor.HttpClientIntroductionAdvice}
 */
public class HttpClient {

    static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    private RxHttpClient httpClient;

    public HttpClient(URL url,
                      HttpClientConfiguration httpClientConfiguration) {
        this.httpClient = new DefaultHttpClient(LoadBalancer.fixed(url),
                httpClientConfiguration,
                null,
                new DefaultThreadFactory(MultithreadEventLoopGroup.class),
                new NettyClientSslBuilder(new ResourceResolver()),
                createDefaultMediaTypeRegistry(),
                AnnotationMetadataResolver.DEFAULT);
        this.httpClient.start();
    }

    private static MediaTypeCodecRegistry createDefaultMediaTypeRegistry() {
        ObjectMapper objectMapper = new ObjectMapperFactory().objectMapper(null, null);
        ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration();
        return MediaTypeCodecRegistry.of(
                new JsonMediaTypeCodec(objectMapper, applicationConfiguration, null),
                new JsonStreamMediaTypeCodec(objectMapper, applicationConfiguration, null)
        );
    }

    public Single<State> state() {
        return httpClient.retrieve(GET("/state"), State.class).singleOrError();
    }

    public Single<State> add(long ordinal) {
        return httpClient.retrieve(POST("/add/"+ordinal, ""), State.class).singleOrError();
    }

    public Single<State> remove(long ordinal) {
        return httpClient.retrieve(POST("/remove/"+ordinal, ""), State.class).singleOrError();
    }

    public Single<Long> writetime(String keyspace, String table, String id) {
        return httpClient.retrieve(
                GET(String.format(Locale.ROOT, "/elasticsearch/%s/%s/%s", keyspace, table, id)),
                Long.class).singleOrError();
    }

    public Single<Long> replicate(String keyspace,
                                  String table,
                                  String id,
                                  Long crc,
                                  UUID nodeId,
                                  String document) {
        return httpClient.retrieve(
                POST(String.format(Locale.ROOT, "/replicate/%s/%s/%s/%s?crc=%d&nodeId=%s",
                        keyspace, table, id, crc, nodeId.toString()),
                        document),
                Long.class).singleOrError();
    }

    public boolean isRunning() {
        return httpClient.isRunning();
    }

    public void close() {
        httpClient.close();
    }
}
