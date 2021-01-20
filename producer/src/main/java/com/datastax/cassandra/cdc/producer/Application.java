package com.datastax.cassandra.cdc.producer;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        try(ApplicationContext context = Micronaut.run(Application.class, args);
            ChangeEventProcessor queueProcessor = context.getBean(ChangeEventProcessor.class);
            CommitLogProcessor commitLogProcessor = context.getBean(CommitLogProcessor.class);
            CommitLogReaderProcessor commitLogReaderProcessor = context.getBean(CommitLogReaderProcessor.class);
        ) {
            queueProcessor.initialize();


            // detect commitlogs file and submit new/modified files to the commitLogReader
            ExecutorService commitLogExecutor = Executors.newSingleThreadExecutor();
            commitLogExecutor.submit(() -> {
                try {
                    commitLogProcessor.initialize();
                    commitLogProcessor.start();
                } catch(Exception e) {
                    logger.error("commitLogProcessor error:", e);
                }
            });

            // wait for the synced position
            commitLogReaderProcessor.awaitSyncedPosition();

            // continuously read commitlogs
            ExecutorService commitLogReaderExecutor = Executors.newSingleThreadExecutor();
            commitLogReaderExecutor.submit(() -> {
                try {
                    commitLogReaderProcessor.initialize();
                    commitLogReaderProcessor.start();
                } catch(Exception e) {
                    logger.error("commitLogReaderProcessor error:", e);
                }
            });

            // process mutations
            try {
                while (true) {
                    queueProcessor.process();
                }
            } catch(InterruptedException e) {
                logger.error("error:", e);
            } finally {
                commitLogExecutor.shutdownNow();
                commitLogReaderExecutor.shutdownNow();
            }
        } catch(Throwable e) {
            logger.error("error:", e);
        }
    }
}
