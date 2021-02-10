package com.datastax.cassandra.cdc.consumer;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/**
 * Pool of single threaded scheduler to sequentially execute sync for mutations having the same hash % schedulers.size().
 */
@Singleton
public class SchedulerPool {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerPool.class);

    List<Scheduler> schedulers;

    SchedulerPool(QuasarConfiguration quasarConfiguration) {
        this.schedulers = new ArrayList<>(quasarConfiguration.consumerThreads);
        for(int i = 0; i < quasarConfiguration.consumerThreads; i++) {
            this.schedulers.add(Schedulers.single());
        }
    }

    Scheduler getScheduler(Integer hash) {
        logger.debug("hash={} schedulers[{}]", hash, Math.abs(hash) % schedulers.size());
        return schedulers.get(Math.abs(hash) % schedulers.size());
    }
}
