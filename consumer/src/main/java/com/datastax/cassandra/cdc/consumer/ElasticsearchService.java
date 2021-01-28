package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.Metrics;
import com.datastax.cassandra.cdc.MutationKey;
import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.CachePut;
import io.micronaut.cache.annotation.Cacheable;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An asynchronous cache<String, Long> where key is the document id
 * (A string representation of the Cassandra primary key), and value
 * is the last writime. The writetime is negative for tombstones.
 */
@Singleton
@CacheConfig("elasticache")
public class ElasticsearchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchService.class);

    public static final String TIMESTAMP_FIELD_NAME = "writetime";
    public static final String[] TIMESTAMP_FIELD_NAME_ARRAY = new String[]{ TIMESTAMP_FIELD_NAME };
    public static final Long DOCUMENT_DOES_NOT_EXIST = -1L;

    final RestHighLevelClient restHighLevelClient;
    final MeterRegistry meterRegistry;

    public ElasticsearchService(RestHighLevelClient restHighLevelClient,
                                MeterRegistry meterRegistry) {
        this.restHighLevelClient = restHighLevelClient;
        this.meterRegistry = meterRegistry;
    }

    /**
     * fetch asynchronously the writetime from elasticsearch
     */
    @Cacheable
    public CompletableFuture<Long> getWritetime(MutationKey eventKey) {
        final String indexName = indexName(eventKey);
        GetRequest request = new GetRequest(indexName, eventKey.id());
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, TIMESTAMP_FIELD_NAME_ARRAY, Strings.EMPTY_ARRAY);
        request.fetchSourceContext(fetchSourceContext);
        request.realtime(true);
        request.refresh(true);
        final CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse response) {
                logger.debug("Get index={} id={} {}", indexName, eventKey.id(), response.getSource());
                meterRegistry.counter(Metrics.METRICS_PREFIX + "es_cache_miss", eventKey.tags()).increment();
                if (response.isExists()) {
                    future.complete((Long)response.getSource().get(TIMESTAMP_FIELD_NAME));
                } else {
                    future.complete(DOCUMENT_DOES_NOT_EXIST);
                }
            }

            @Override
            public void onFailure(Exception e) {
                meterRegistry.counter(Metrics.METRICS_PREFIX + "es_cache_miss_failure", eventKey.tags()).increment();
                if (e instanceof ElasticsearchStatusException) {
                    ElasticsearchStatusException ese = (ElasticsearchStatusException) e;
                    if (RestStatus.NOT_FOUND.equals(ese.status())) {
                        future.complete( DOCUMENT_DOES_NOT_EXIST );
                        return;
                    }
                    logger.warn("Elasticsearch failed to get index={} id={} status={}", indexName, eventKey.id(), ese.status());
                } else {
                    logger.warn("Failed to get index={} id={} status={}", indexName, eventKey.id(), e);
                }
                future.completeExceptionally(e);
            }
        });
        return future.thenComposeAsync(wt -> (wt == DOCUMENT_DOES_NOT_EXIST) ? lookupTombstone(eventKey) : CompletableFuture.completedFuture(wt));
    }

    CompletableFuture<Long> lookupTombstone(final MutationKey eventKey) {
        final String tombstoneIndex = tombstoneIndexWilcard(eventKey);
        SearchRequest searchRequest = new SearchRequest(tombstoneIndex);
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds(eventKey.id()));
        searchSourceBuilder.sort(TIMESTAMP_FIELD_NAME, SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        final CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                if (searchResponse.getHits().getTotalHits().value > 0) {
                    logger.debug("Tombstone found index={} id={} source={}",
                            tombstoneIndex, eventKey.id(), searchResponse.getHits().getAt(0).getSourceAsString());
                    future.complete((Long)searchResponse.getHits().getAt(0).getSourceAsMap().get(TIMESTAMP_FIELD_NAME) * -1L);
                } else {
                    future.complete(DOCUMENT_DOES_NOT_EXIST);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to get tombstone index={} id={}", tombstoneIndex, eventKey.id(), e);
                future.complete(DOCUMENT_DOES_NOT_EXIST);
            }
        });
        return future;
    }

    @CachePut(parameters = {"pk"}, async = true)
    public CompletableFuture<Long> index(final MutationKey eventKey, final Long writetime, final Map<String, Object> source) throws IOException {
        final String indexName = indexName(eventKey);
        IndexRequest request = new IndexRequest(indexName);
        request.id(eventKey.id());
        source.put(TIMESTAMP_FIELD_NAME, writetime);
        request.source(source, XContentType.JSON);
        CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                logger.info("Indexed index={} id={} source={}", indexName, eventKey.id(), source);
                meterRegistry.counter(Metrics.METRICS_PREFIX + "es_index", eventKey.tags()).increment();
                future.complete(writetime);
            }

            @Override
            public void onFailure(Exception e) {
                meterRegistry.counter(Metrics.METRICS_PREFIX + "es_index_error", eventKey.tags()).increment();
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    /**
     * Delete the elasticsearch document and index a tombestone.
     **/
    @CachePut(parameters = {"pk"}, async = true)
    public CompletableFuture<Long> delete(final MutationKey eventKey, final Long writetime) throws IOException {
        String indexName = indexName(eventKey);
        DeleteRequest request = new DeleteRequest(indexName);
        request.id(eventKey.id());
        CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse indexResponse) {
                logger.info("Deleted index={} id={}", indexName, eventKey.id());
                meterRegistry.counter(Metrics.METRICS_PREFIX + "es_delete", eventKey.tags()).increment();
                future.complete(writetime);
            }

            @Override
            public void onFailure(Exception e) {
                meterRegistry.counter(Metrics.METRICS_PREFIX + "es_delete_error", eventKey.tags()).increment();
                future.completeExceptionally(e);
            }
        });
        return future.thenComposeAsync(wt -> indexTombstone(eventKey, wt));
    }

    CompletableFuture<Long> indexTombstone(final MutationKey eventKey, final Long writetime) {
        final String tombstoneIndex = tombstoneIndexName(eventKey, writetime);
        IndexRequest request = new IndexRequest(tombstoneIndex);
        request.id(eventKey.id());
        request.source(ImmutableMap.of(TIMESTAMP_FIELD_NAME, writetime), XContentType.JSON);
        final CompletableFuture<Long> future = new CompletableFuture<>();
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                logger.info("Indexed tombstone index={} id={} writetime={}", tombstoneIndex, eventKey.id(), writetime);
                future.complete(-writetime);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    String indexName(MutationKey eventKey) {
        return eventKey.getKeyspace().toLowerCase(Locale.ROOT)+
                "-"+eventKey.getTable().toLowerCase(Locale.ROOT);
    }

    String tombstoneIndexWilcard(MutationKey eventKey) {
        return "tombstone-"+eventKey.getKeyspace().toLowerCase(Locale.ROOT)+
                "-"+eventKey.getTable().toLowerCase(Locale.ROOT)+
                "-*";
    }

    String tombstoneIndexName(MutationKey eventKey, final Long writetime) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        return "tombstone-"+eventKey.getKeyspace().toLowerCase(Locale.ROOT)+
                "-"+eventKey.getTable().toLowerCase(Locale.ROOT)+
                "-"+simpleDateFormat.format(new Date(writetime));
    }
}
