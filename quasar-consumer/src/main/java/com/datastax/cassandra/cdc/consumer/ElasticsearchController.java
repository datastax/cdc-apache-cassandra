package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.ElasticsearchService;
import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.consumer.exceptions.HashNotManagedException;
import com.datastax.cassandra.cdc.consumer.exceptions.ServiceNotRunningException;
import com.datastax.cassandra.cdc.quasar.Murmur3HashFunction;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.NotBlank;
import javax.annotation.Nullable;

@Controller("/elasticsearch")
public class ElasticsearchController {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchController.class);

    @Inject
    ElasticsearchService elasticsearchService;

    @Inject
    QuasarClusterManager quasarClusterManager;

    @Inject
    MeterRegistry meterRegistry;

    /**
     * Retrieves the writetime for a given Elasticsearch document.
     * @param keyspace
     * @param table
     * @param id
     * @return
     * @throws HashNotManagedException
     * @throws ServiceNotRunningException
     */
    @Get(value = "/{keyspace}/{table}/{id}", produces = MediaType.APPLICATION_JSON)
    public Single<ElasticsearchService.CacheValue> getWritetime(
            @NotBlank @QueryValue("keyspace") String keyspace,
            @NotBlank @QueryValue("table") String table,
            @NotBlank @QueryValue("id") String id)
            throws HashNotManagedException, ServiceNotRunningException {
        final MutationKey mutationKey = new MutationKey(keyspace, table, id);
        final int hash = mutationKey.hash();
        quasarClusterManager.checkHash(hash);
        meterRegistry.counter("es_get_writetime", mutationKey.tags()).increment();
        return Single.fromFuture(elasticsearchService.getWritetime(mutationKey));
    }
}
