package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.MutationKey;
import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Tag;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.reactivex.Single;

import javax.inject.Inject;

@Controller("/elasticsearch")
public class ElasticsearchController {
    @Inject
    ElasticsearchService elasticsearchService;

    @Get(value = "/{keyspace}/{table}/{id}")
    public Single<Long> getWritetime(String keyspace, String table, String id) {
        Iterable<Tag> tags = ImmutableList.of(Tag.of("keyspace", keyspace), Tag.of("table", table));
        return Single.fromFuture(elasticsearchService.getWritetime(new MutationKey(keyspace,table, id)));
    }
}
