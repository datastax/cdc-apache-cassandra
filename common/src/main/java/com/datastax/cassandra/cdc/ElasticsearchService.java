package com.datastax.cassandra.cdc;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.cache.annotation.CacheConfig;
import io.micronaut.cache.annotation.CachePut;
import io.micronaut.cache.annotation.Cacheable;
import lombok.*;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An asynchronous cache<MutationKey, CacheValue> where key is the MutationKey, and value
 * is a CacheValue object, where writetime is negative if read ALL failed.
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

    @EqualsAndHashCode
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class CacheValue {
        boolean isTombstone;
        long writetime;

        static final CacheValue DOCUMENT_DOES_NOT_EXIST = new CacheValue(false, ElasticsearchService.DOCUMENT_DOES_NOT_EXIST);

        /**
         * Writetime is stored as a positive number when read ALL succeed, negative otherwise.
         * @return
         */
        public boolean isConsistent() {
            return writetime > 0;
        }

        public long absoluteWritetime() {
            return Math.abs(writetime);
        }
    }

    /**
     * fetch asynchronously the writetime from elasticsearch
     */
    @Cacheable
    public CompletableFuture<CacheValue> getWritetime(MutationKey pk) {
        final String indexName = indexName(pk);
        GetRequest request = new GetRequest(indexName, pk.id());
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, TIMESTAMP_FIELD_NAME_ARRAY, Strings.EMPTY_ARRAY);
        request.fetchSourceContext(fetchSourceContext);
        request.realtime(true);
        request.refresh(true);
        final CompletableFuture<CacheValue> future = new CompletableFuture<>();
        restHighLevelClient.getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse response) {
                logger.debug("Get index={} id={} {}", indexName, pk.id(), response.getSource());
                meterRegistry.counter(MetricConstants.METRICS_PREFIX + "es_cache_miss", pk.tags()).increment();
                if (response.isExists()) {
                    future.complete(new CacheValue(false, (Long)response.getSource().get(TIMESTAMP_FIELD_NAME)));
                } else {
                    future.complete(CacheValue.DOCUMENT_DOES_NOT_EXIST);
                }
            }

            @Override
            public void onFailure(Exception e) {
                meterRegistry.counter(MetricConstants.METRICS_PREFIX + "es_cache_miss_failure", pk.tags()).increment();
                if (e instanceof ElasticsearchStatusException) {
                    ElasticsearchStatusException ese = (ElasticsearchStatusException) e;
                    if (RestStatus.NOT_FOUND.equals(ese.status())) {
                        future.complete( CacheValue.DOCUMENT_DOES_NOT_EXIST );
                        return;
                    }
                    logger.warn("Elasticsearch failed to get index={} id={} status={}", indexName, pk.id(), ese.status());
                } else {
                    logger.warn("Failed to get index={} id={} status={}", indexName, pk.id(), e);
                }
                future.completeExceptionally(e);
            }
        });
        return future.thenComposeAsync(wt -> (wt == CacheValue.DOCUMENT_DOES_NOT_EXIST) ? lookupTombstone(pk) : CompletableFuture.completedFuture(wt));
    }

    CompletableFuture<CacheValue> lookupTombstone(final MutationKey eventKey) {
        final String tombstoneIndex = tombstoneIndexWilcard(eventKey);
        SearchRequest searchRequest = new SearchRequest(tombstoneIndex);
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        searchSourceBuilder.query(QueryBuilders.idsQuery().addIds(eventKey.id()));
        searchSourceBuilder.sort(TIMESTAMP_FIELD_NAME, SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);
        final CompletableFuture<CacheValue> future = new CompletableFuture<>();
        restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                if (searchResponse.getHits().getTotalHits().value > 0) {
                    logger.debug("Tombstone found index={} id={} source={}",
                            tombstoneIndex, eventKey.id(), searchResponse.getHits().getAt(0).getSourceAsString());
                    future.complete(new CacheValue(true, (Long)searchResponse.getHits().getAt(0).getSourceAsMap().get(TIMESTAMP_FIELD_NAME) * -1L));
                } else {
                    future.complete(CacheValue.DOCUMENT_DOES_NOT_EXIST);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to get tombstone index={} id={}", tombstoneIndex, eventKey.id(), e);
                future.complete(CacheValue.DOCUMENT_DOES_NOT_EXIST);
            }
        });
        return future;
    }

    @CachePut(parameters = {"pk"}, async = true)
    public CompletableFuture<CacheValue> index(final MutationKey pk, final Long writetime, final Map<String, Object> source) {
        final String indexName = indexName(pk);
        IndexRequest request = new IndexRequest(indexName);
        request.id(pk.id());
        source.put(TIMESTAMP_FIELD_NAME, writetime);
        request.source(source, XContentType.JSON);
        CompletableFuture<CacheValue> future = new CompletableFuture<>();
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                logger.info("Indexed index={} id={} source={}", indexName, pk.id(), source);
                meterRegistry.counter(MetricConstants.METRICS_PREFIX + "es_index", pk.tags()).increment();
                future.complete(new CacheValue(false, writetime));
            }

            @Override
            public void onFailure(Exception e) {
                meterRegistry.counter(MetricConstants.METRICS_PREFIX + "es_index_error", pk.tags()).increment();
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    /**
     * Delete the elasticsearch document and index a tombestone.
     **/
    @CachePut(parameters = {"pk"}, async = true)
    public CompletableFuture<CacheValue> delete(final MutationKey pk, final Long writetime) {
        String indexName = indexName(pk);
        DeleteRequest request = new DeleteRequest(indexName);
        request.id(pk.id());
        CompletableFuture<CacheValue> future = new CompletableFuture<>();
        restHighLevelClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse indexResponse) {
                logger.info("Deleted index={} id={}", indexName, pk.id());
                meterRegistry.counter(MetricConstants.METRICS_PREFIX + "es_delete", pk.tags()).increment();
                future.complete(new CacheValue(true, writetime));
            }

            @Override
            public void onFailure(Exception e) {
                meterRegistry.counter(MetricConstants.METRICS_PREFIX + "es_delete_error", pk.tags()).increment();
                future.completeExceptionally(e);
            }
        });
        return future.thenComposeAsync(cv -> indexTombstone(pk, cv.writetime));
    }

    CompletableFuture<CacheValue> indexTombstone(final MutationKey eventKey, final Long writetime) {
        final String tombstoneIndex = tombstoneIndexName(eventKey, writetime);
        IndexRequest request = new IndexRequest(tombstoneIndex);
        request.id(eventKey.id());
        request.source(ImmutableMap.of(TIMESTAMP_FIELD_NAME, writetime), XContentType.JSON);
        final CompletableFuture<CacheValue> future = new CompletableFuture<>();
        restHighLevelClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                logger.info("Indexed tombstone index={} id={} writetime={}", tombstoneIndex, eventKey.id(), writetime);
                future.complete(new CacheValue(true, writetime));
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
