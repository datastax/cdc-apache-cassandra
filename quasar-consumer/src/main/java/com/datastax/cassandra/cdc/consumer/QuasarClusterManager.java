package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.CassandraService;
import com.datastax.cassandra.cdc.ElasticsearchService;
import com.datastax.cassandra.cdc.consumer.exceptions.HashNotManagedException;
import com.datastax.cassandra.cdc.consumer.exceptions.ServiceNotRunningException;
import com.datastax.cassandra.cdc.quasar.HttpClientFactory;
import com.datastax.cassandra.cdc.quasar.State;
import com.datastax.cassandra.cdc.quasar.Status;
import com.datastax.oss.driver.api.core.cql.Row;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.aop.Around;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
@Around(lazy = false)
public class QuasarClusterManager {
    private static final Logger logger = LoggerFactory.getLogger(QuasarClusterManager.class);

    final QuasarConfiguration quasarConfiguration;
    final CassandraService cassandraService;
    final ElasticsearchService elasticsearchService;
    final MeterRegistry meterRegistry;

    final AtomicReference<State> stateAtomicReference;

    final HttpClientFactory httpClientFactory;

    public QuasarClusterManager(QuasarConfiguration configuration,
                                CassandraService cassandraService,
                                ElasticsearchService elasticsearchService,
                                HttpClientFactory httpClientFactory,
                                MeterRegistry meterRegistry) {
        this.quasarConfiguration = configuration;
        this.cassandraService = cassandraService;
        this.elasticsearchService = elasticsearchService;
        this.stateAtomicReference = new AtomicReference<>(State.NO_STATE);
        this.meterRegistry = meterRegistry;
        this.httpClientFactory = httpClientFactory;
    }

    @PostConstruct
    public void init() {
        cassandraService.getSession()
                .thenApplyAsync(session ->
                        session.executeAsync(String.format(Locale.ROOT,
                                "CREATE KEYSPACE IF NOT EXISTS quasar WITH replication = {'class': 'NetworkTopologyStrategy', '%s':'1'};",
                                quasarConfiguration.defaultDatacenter))
                                .thenCompose(x -> session.executeAsync("CREATE TABLE IF NOT EXISTS quasar.metadata (" +
                                        "cluster_name text, " +
                                        "size int," +
                                        "ts timestamp," +
                                        "v bigint," +
                                        "version bigint static," +
                                        "PRIMARY KEY((cluster_name), v)) WITH CLUSTERING ORDER BY (v DESC)"))
                                .thenCompose(x -> session.executeAsync("INSERT INTO quasar.metadata " +
                                                "(cluster_name, version, v, size, ts) VALUES(?,0,0,0,dateOf(now())) IF NOT EXISTS",
                                        quasarConfiguration.clusterName))
                                .thenCompose(x -> session.executeAsync("SELECT size, v FROM quasar.metadata WHERE cluster_name = ? LIMIT 1",
                                        quasarConfiguration.clusterName))
                                .thenCompose(rs -> {
                                    Row row = rs.one();
                                    int size = row.getInt("size");
                                    long version = row.getLong("v");
                                    Status status = size > quasarConfiguration.ordinal ? Status.RUNNING : Status.JOINING;
                                    this.stateAtomicReference.set(new State(status, size, version));
                                    logger.debug("Row cluster_name={} state={}", quasarConfiguration.clusterName, stateAtomicReference.get());
                                    return status.equals(Status.JOINING) ? doJoin() : CompletableFuture.completedFuture(stateAtomicReference.get());
                                })
                                .whenComplete((state, error) -> {
                                    if(error != null) {
                                        logger.warn("Failed to initialize: {}", error);
                                    } else {
                                        logger.info("Successfully started state={}", state);
                                    }
                                }));
    }

    CompletionStage<State> increaseSize() {
        return cassandraService.getSession()
                .thenCompose(session -> {
                    final State state = stateAtomicReference.get();
                    final long newVersion = state.getVersion() + 1;
                    final int newSize = state.getSize() + 1;
                    return session.executeAsync("UPDATE quasar.metadata SET version = ?, size =? , ts = ? WHERE cluster_name = ? AND v = ? IF version = ?",
                            newVersion, newSize, Instant.now(), quasarConfiguration.clusterName, newVersion, state.getVersion())
                            .thenApply(rs -> {
                                if(rs.wasApplied()) {
                                    stateAtomicReference.set(new State(Status.RUNNING, newSize, newVersion));
                                }
                                logger.info("applied={} old_state={} currentState={}",
                                        rs.wasApplied(), state, stateAtomicReference.get());
                                return stateAtomicReference.get();
                            });
                });
    }

    CompletionStage<State> decreaseSize() {
        return cassandraService.getSession()
                .thenCompose(session -> {
                    final State state = stateAtomicReference.get();
                    final long newVersion = state.getVersion() + 1;
                    final int newSize = state.getSize() - 1;
                    return session.executeAsync("UPDATE quasar.metadata SET version = ?, size =? , ts = ? WHERE cluster_name = ? AND v = ? IF version = ?",
                            newVersion, newSize, Instant.now(), quasarConfiguration.clusterName, newVersion, state.getVersion())
                            .thenApply(rs -> {
                                if(rs.wasApplied()) {
                                    stateAtomicReference.set(new State(Status.LEFT, newSize, newVersion));
                                }
                                logger.info("applied={} old_state={} currentState={}",
                                        rs.wasApplied(), state, stateAtomicReference.get());
                                return stateAtomicReference.get();
                            });
                });
    }

    /**
     * notify all members and update the cluster size if succeed.
     */
    public Single<Map<Integer, State>> cluster() {
        List<Single<State>> notifications = new ArrayList<>();
        for(int i = 0; i < stateAtomicReference.get().getSize(); i++) {
            final int j = i;
            if (j == quasarConfiguration.ordinal) {
                notifications.add(Single.just(stateAtomicReference.get()));
            } else {
                try {
                    notifications.add(
                            httpClientFactory.clientForOrdinal(i, false)
                                    .state()
                                    .subscribeOn(Schedulers.io())
                                    .map(vs -> {
                                        logger.info("node[{}]={}", j, vs);
                                        return vs;
                                    })
                                    .onErrorReturnItem(State.NO_STATE)
                    );
                } catch(Exception ex) {
                    logger.error("error:", ex);
                }
            }
        }
        return Single.zip(notifications, (array) -> {
            Map<Integer, State> map = new HashMap<>(array.length);
            for(int i=0; i < array.length; i++) {
                map.put(i, (State) array[i]);
            }
            return map;
        });
    }

    /**
     * notify all members and update the cluster size if succeed.
     */
    public CompletionStage<State> doJoin() {
        logger.debug("Joining the cluster={} with name={}", quasarConfiguration.clusterName, quasarConfiguration.nodeName());
        List<Completable> notifications = new ArrayList<>();
        for(int i = 0; i < quasarConfiguration.ordinal; i++) {
            final int j = i;
            try {
                notifications.add(
                        httpClientFactory.clientForOrdinal(i, false)
                                .add(quasarConfiguration.ordinal)
                                .subscribeOn(Schedulers.io())
                                .map(vs -> {
                                    logger.info("node[{}]={}", j, vs);
                                    return vs;
                                }).ignoreElement()
                );
            } catch(Exception ex) {
                logger.error("error:", ex);
            }
        }
        CompletableFuture<State> cf = new CompletableFuture<>();
        Completable.merge(notifications)
                .andThen(Completable.fromFuture(increaseSize().toCompletableFuture()))
                .subscribe(() -> {
                    logger.info("All notification succeed to add ordinal={}", quasarConfiguration.ordinal);
                    cf.complete(stateAtomicReference.get());
                }, t -> {
                    logger.error("add notification failed:", t);
                    cf.completeExceptionally(t);
                });
        return cf;
    }

    /**
     * Decrement the cluster size by one, notify all cluster member to update, and stop.
     */
    public CompletionStage<State> doLeave() {
        logger.debug("Leaving the cluster={} ordinal={}", quasarConfiguration.clusterName, quasarConfiguration.ordinal);
        List<Completable> notifications = new ArrayList<>();
        for(int i = 0; i < quasarConfiguration.ordinal; i++) {
            final int j = i;
            try {
                notifications.add(
                        httpClientFactory.clientForOrdinal(j, false)
                                .remove(quasarConfiguration.ordinal)
                                .subscribeOn(Schedulers.io())
                                .map(vs -> {
                                    logger.info("node[{}]={}", j, vs);
                                    return vs;
                                }).ignoreElement()
                );
            } catch(Exception ex) {
                logger.error("error:", ex);
            }
        }
        CompletableFuture<State> cf = new CompletableFuture<>();
        Completable.merge(notifications)
                .andThen(Completable.fromFuture(decreaseSize().toCompletableFuture()))
                .subscribe(() -> {
                    logger.info("All notification succeed to remove ordinal={}", quasarConfiguration.ordinal);
                    cf.complete(stateAtomicReference.get());
                }, t -> {
                    logger.error("remove notification failed:", t);
                    cf.completeExceptionally(t);
                });
        return cf;
    }

    public Single<State> add(Integer ordinal) {
        return Single.fromCallable(new Callable<State>() {
            @Override
            public State call() throws Exception {
                if (ordinal < stateAtomicReference.get().getSize()) {
                    logger.info("ordinal={} already a member, size={}", ordinal, stateAtomicReference.get());
                    return stateAtomicReference.get();
                }
                // try to increase size.
                State currentState = stateAtomicReference.get();
                if (stateAtomicReference.compareAndSet(
                        currentState,
                        new State(currentState.getStatus(), ordinal+1, currentState.getVersion() + 1))) {
                    logger.info("added ordinal={} size={}", ordinal, stateAtomicReference.get());
                    return stateAtomicReference.get();
                }
                throw new IllegalArgumentException("Failed to add ordinal=" + ordinal);
            }
        });
    }

    public Single<State> remove(Integer ordinal) {
        return Single.fromCallable(new Callable<State>() {
            @Override
            public State call() throws Exception {
                if (ordinal >= stateAtomicReference.get().getSize()) {
                    logger.info("ordinal={} already not a member, size={}", ordinal, stateAtomicReference.get().getSize());
                    return stateAtomicReference.get();
                }
                State current = stateAtomicReference.get();
                State next = current.toBuilder().size(current.getSize() - 1).version(current.getVersion() + 1).build();
                if (stateAtomicReference.compareAndSet(current, next)) {
                    logger.info("removed ordinal={} state={}", ordinal, stateAtomicReference.get());
                    return stateAtomicReference.get();
                }
                throw new IllegalArgumentException("Cannot remove ordinal=" + ordinal);
            }
        });
    }

    public Integer ordinalForHash(int hash) {
        return Math.abs(hash) % this.stateAtomicReference.get().getSize();
    }

    public boolean manage(int hash) {
        return ordinalForHash(hash) == quasarConfiguration.ordinal;
    }

    public void checkStatus() throws ServiceNotRunningException {
        if (!Status.RUNNING.equals(stateAtomicReference.get().getStatus())) {
            throw new ServiceNotRunningException(stateAtomicReference.get());
        }
    }

    public void checkHash(Integer hash) throws HashNotManagedException, ServiceNotRunningException {
        checkStatus();
        if (hash != null && !manage(hash)) {
            throw new HashNotManagedException(stateAtomicReference.get(), hash);
        }
    }
}
