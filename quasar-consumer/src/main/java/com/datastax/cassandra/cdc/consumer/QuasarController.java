package com.datastax.cassandra.cdc.consumer;

import com.datastax.cassandra.cdc.quasar.Murmur3HashFunction;
import com.datastax.cassandra.cdc.MutationKey;
import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.cassandra.cdc.Operation;
import com.datastax.cassandra.cdc.consumer.exceptions.HashNotManagedException;
import com.datastax.cassandra.cdc.consumer.exceptions.ServiceNotRunningException;
import com.datastax.cassandra.cdc.quasar.State;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.http.annotation.Error;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import java.util.Map;
import java.util.UUID;

@Controller
public class QuasarController {
    private static final Logger logger = LoggerFactory.getLogger(QuasarController.class);

    final QuasarConfiguration quasarConfiguration;
    final QuasarConsumer quasarConsumer;
    final QuasarClusterManager quasarClusterManager;
    final SchedulerPool schedulerPool;

    public QuasarController(QuasarConfiguration quasarConfiguration,
                            QuasarClusterManager clusterManager,
                            QuasarConsumer quasarConsumer,
                            SchedulerPool schedulerPool) {
        this.quasarConsumer = quasarConsumer;
        this.quasarConfiguration = quasarConfiguration;
        this.quasarClusterManager = clusterManager;
        this.schedulerPool = schedulerPool;
    }

    /**
     * Returns the ordinal number of the node.
     * @return the node ordinal
     */
    @Get(value = "/ordinal")
    public Single<Integer> ordinal() {
        return Single.just(quasarConfiguration.ordinal());
    }

    /**
     * Returns the ordinal for the provided hash.
     * @return the ordinal managing the provided hash
     */
    @Get(value = "/ordinal/{hash}" , produces = MediaType.APPLICATION_JSON)
    public Single<Integer> ordinal(@NotBlank @QueryValue("hash") Integer hash) {
        return Single.just(quasarClusterManager.ordinal(hash));
    }

    /**
     * Returns the node state.
     * @return the node state
     */
    @Get(value = "/state")
    public Single<State> state() {
        return Single.just(quasarClusterManager.stateAtomicReference.get());
    }

    /**
     * Return the state of all nodes in the cluster.
     * @return the cluster state map
     */
    @Get(value = "/cluster" , produces = MediaType.APPLICATION_JSON)
    public Single<Map<Integer, State>> cluster() {
        return quasarClusterManager.cluster();
    }

    /**
     * Leave the cluster.
     * @return the node final state
     */
    @Post(value="/cluster/_leave" , produces = MediaType.APPLICATION_JSON)
    public Single<State> leave() {
        if (quasarClusterManager.stateAtomicReference.get().getSize() - 1 != quasarConfiguration.ordinal()) {
            throw new IllegalStateException("Can only remove the last node at ordinal="+(quasarClusterManager.stateAtomicReference.get().getSize() - 1));
        }
        return Single.fromFuture(quasarClusterManager.doLeave().toCompletableFuture());
    }

    /**
     * Re-join the cluster.
     * @return the node final state
     */
    @Post(value="/cluster/_join" , produces = MediaType.APPLICATION_JSON)
    public Single<State> join() {
        if (quasarClusterManager.stateAtomicReference.get().getSize() != quasarConfiguration.ordinal()) {
            throw new IllegalStateException("Can only add the last node at ordinal="+(quasarClusterManager.stateAtomicReference.get().getSize()));
        }
        return Single.fromFuture(quasarClusterManager.doJoin().toCompletableFuture());
    }

    /**
     * Add the node at the provided ordinal position.
     * Used by the coordinator node to join the cluster.
     * @param ordinal
     * @return
     */
    @Post(value="/add/{ordinal}" , produces = MediaType.APPLICATION_JSON)
    public Single<State> add(@NotBlank @QueryValue("ordinal") Integer ordinal) {
        return quasarClusterManager.add(ordinal);
    }

    /**
     * Remove the node at the provided ordinal position.
     * Used by the coordinator node to leave the cluster.
     * @param ordinal
     * @return
     */
    @Post(value="/remove/{ordinal}" , produces = MediaType.APPLICATION_JSON)
    public Single<State> remove(@NotBlank @QueryValue("ordinal") Integer ordinal) {
        return quasarClusterManager.remove(ordinal);
    }

    /**
     * Read from Cassandra and insert/delete the corresponding Elasticsearch document.
     * @param keyspace
     * @param table
     * @param id
     * @param writetime
     * @param nodeId
     * @return
     * @throws ServiceNotRunningException
     * @throws HashNotManagedException
     */
    @Get(value = "/replicate/{keyspace}/{table}/{id}",
            produces = MediaType.TEXT_PLAIN)
    public Single<Long> readAndReplicate(@NotBlank @QueryValue("keyspace") String keyspace,
                               @NotBlank @QueryValue("table") String table,
                               @NotBlank @QueryValue("id") String id,
                               @NotBlank @QueryValue("writetime") Long writetime,
                               @Nullable @QueryValue("nodeId") UUID nodeId)
            throws ServiceNotRunningException, HashNotManagedException
    {
        int hash = Murmur3HashFunction.hash(id);
        quasarClusterManager.checkHash(hash);
        return Single.fromFuture(quasarConsumer.consume(
                new MutationKey(keyspace, table, id).parseId(),
                new MutationValue(writetime, nodeId, Operation.INSERT, null)),
                schedulerPool.getScheduler(hash));
    }

    /**
     * Index an Elasticsearch document if the mutations is not obsolete according to the writetime.
     * @param keyspace
     * @param table
     * @param id
     * @param writetime
     * @param nodeId
     * @param document
     * @return
     * @throws ServiceNotRunningException
     * @throws HashNotManagedException
     */
    @Post(value = "/replicate/{keyspace}/{table}/{id}",
            consumes = MediaType.APPLICATION_JSON,
            produces = MediaType.TEXT_PLAIN)
    public Single<Long> upsert(@NotBlank @QueryValue("keyspace") String keyspace,
                                  @NotBlank @QueryValue("table") String table,
                                  @NotBlank @QueryValue("id") String id,
                                  @NotBlank @QueryValue("writetime") Long writetime,
                                  @Nullable @QueryValue("nodeId") UUID nodeId,
                                  @Body String document)
            throws ServiceNotRunningException, HashNotManagedException
    {
        int hash = Murmur3HashFunction.hash(id);
        quasarClusterManager.checkHash(hash);
        return Single.fromFuture(quasarConsumer.consume(
                    new MutationKey(keyspace, table, id),
                    new MutationValue(writetime, nodeId, Operation.INSERT, document)),
                    schedulerPool.getScheduler(hash));
    }

    /**
     * Delete an Elasticsearch document if not the mutation is not obsolete according to the writetime.
     * @param keyspace
     * @param table
     * @param id
     * @param writetime
     * @param nodeId
     * @return
     * @throws ServiceNotRunningException
     * @throws HashNotManagedException
     */
    @Delete(value = "/replicate/{keyspace}/{table}/{id}",
            produces = MediaType.TEXT_PLAIN)
    public Single<Long> delete(@NotBlank @QueryValue("keyspace") String keyspace,
                                  @NotBlank @QueryValue("table") String table,
                                  @NotBlank @QueryValue("id") String id,
                                  @NotBlank @QueryValue("writetime") Long writetime,
                                  @Nullable @QueryValue("nodeId") UUID nodeId)
            throws ServiceNotRunningException, HashNotManagedException
    {
        int hash = Murmur3HashFunction.hash(id);
        quasarClusterManager.checkHash(hash);
        return Single.fromFuture(quasarConsumer.consume(
                new MutationKey(keyspace, table, id),
                new MutationValue(writetime, nodeId, Operation.DELETE, null)),
                schedulerPool.getScheduler(hash));
    }

    /**
     * Global error handler
     * @param request
     * @param e
     * @return
     */
    @Error(global = true)
    public HttpResponse<State> error(HttpRequest<?> request, Throwable e) {
        logger.error("Error:", e);

        if(e instanceof ServiceNotRunningException) {
            ServiceNotRunningException notRunningException = (ServiceNotRunningException) e;
            return HttpResponse.<Long>serverError()
                    .status(HttpStatus.SERVICE_UNAVAILABLE, "Consumer service status=" + notRunningException.state.getStatus())
                    .header("Retry-After", Integer.toString(10))
                    .body(quasarClusterManager.stateAtomicReference.get());
        }

        if (e instanceof HashNotManagedException) {
            HashNotManagedException hashNotManagedException = (HashNotManagedException)e;
            return HttpResponse.<Long>serverError()
                        .status(HttpStatus.NOT_FOUND, "Not managed hash=" + hashNotManagedException.hash)
                        .body(quasarClusterManager.stateAtomicReference.get());
        }

        return HttpResponse.<Long>serverError()
                .status(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage())
                .body(quasarClusterManager.stateAtomicReference.get());
    }
}
