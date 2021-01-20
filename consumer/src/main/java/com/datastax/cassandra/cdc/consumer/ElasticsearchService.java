package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.EventKey;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.CachePut;
import io.micronaut.cache.annotation.Cacheable;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Singleton
@CacheConfig("elasticache")
public class ElasticsearchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    public static final String TIMESTAMP_FIELD_NAME = "writetime";
    public static final String[] TIMESTAMP_FIELD_NAME_ARRAY = new String[]{ TIMESTAMP_FIELD_NAME };
    public static final Long DOCUMENT_DOES_NOT_EXIST = -1L;

    final RestHighLevelClient restHighLevelClient;

    public ElasticsearchService(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * fetch asynchronously the writetime from elasticsearch
     */
    @Cacheable
    public CompletableFuture<Long> getWritetime(EventKey pk) {
        GetRequest request = new GetRequest(
                pk.getKeyspace(),
                //pk.getTable(),
                pk.id());
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, TIMESTAMP_FIELD_NAME_ARRAY, Strings.EMPTY_ARRAY);
        request.fetchSourceContext(fetchSourceContext);
        request.realtime(true);
        request.refresh(true);
        CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse response) {
                logger.debug("Get index={} id={} {}", pk.getKeyspace(), pk.id(), response.getSource());
                if (response.isExists()) {
                    future.complete((Long)response.getSource().get(TIMESTAMP_FIELD_NAME));
                } else {
                    future.complete( DOCUMENT_DOES_NOT_EXIST );
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ElasticsearchStatusException) {
                    ElasticsearchStatusException ese = (ElasticsearchStatusException) e;
                    if (RestStatus.NOT_FOUND.equals(ese.status())) {
                        future.complete(DOCUMENT_DOES_NOT_EXIST);
                        return;
                    }
                    logger.warn("Elasticsearch failed to get {}/{} status={}", pk.getKeyspace(), pk.id(), ese.status());
                } else {
                    logger.warn("Failed to get {}/{} status={}", pk.getKeyspace(), pk.id(), e);
                }
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @CachePut(parameters = {"pk"}, async = true)
    public CompletableFuture<Long> index(final EventKey pk, final Long writetime, final Map<String, Object> source) throws IOException {
        IndexRequest request = new IndexRequest(pk.getKeyspace());
        request.id(pk.id());
        //request.type(kv.getKey().getTable());
        source.put(TIMESTAMP_FIELD_NAME, writetime);
        request.source(source, XContentType.JSON);
        logger.info("Indexing index={} id={} source={}", pk.getKeyspace(), pk.id(), source);
        CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                future.complete(writetime);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }
}
