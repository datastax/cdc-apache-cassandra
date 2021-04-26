package com.datastax.cassandra.cdc.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Agent {
    public static void premain(String agentArgs, Instrumentation inst) {
        log.info("[Agent] In premain method");
        try {
            main(agentArgs, inst);
        } catch(Exception e) {
            log.error("error:", e);
            System.exit(-1);
        }
    }

    public static void agentmain(String agentArgs, Instrumentation inst) {
        log.info("[Agent] In agentmain method");
        try {
            main(agentArgs, inst);
        } catch(Exception e) {
            log.error("error:", e);
            System.exit(-1);
        }
    }

    static void main(String agentArgs, Instrumentation inst) throws Exception {
        DatabaseDescriptor.daemonInitialization();
        if (DatabaseDescriptor.isCDCEnabled() == false) {
            log.error("cdc_enabled=false in your cassandra configuration, CDC agent not started.");
        } else if (DatabaseDescriptor.getCDCLogLocation() == null) {
            log.error("cdc_raw_directory=null in your cassandra configuration, CDC agent not started.");
        } else {
            startCdcProducer(agentArgs);
        }
    }

    static void startCdcProducer(String agentArgs) throws IOException {
        log.info("Starting CDC producer agent");

        ProducerConfig.configure(agentArgs);

        OffsetFileWriter offsetFileWriter = new OffsetFileWriter(DatabaseDescriptor.getCDCLogLocation());
        KafkaMutationSender kafkaMutationSender = new KafkaMutationSender();
        CommitLogReadHandlerImpl commitLogReadHandler = new CommitLogReadHandlerImpl(offsetFileWriter, kafkaMutationSender);
        CommitLogTransfer commitLogTransfer = new BlackHoleCommitLogTransfer();
        CommitLogReaderProcessor commitLogReaderProcessor = new CommitLogReaderProcessor(commitLogReadHandler, offsetFileWriter, commitLogTransfer);
        CommitLogProcessor commitLogProcessor = new CommitLogProcessor(DatabaseDescriptor.getCDCLogLocation(), commitLogTransfer, offsetFileWriter, commitLogReaderProcessor);

        // detect commitlogs file and submit new/modified files to the commitLogReader
        ExecutorService commitLogExecutor = Executors.newSingleThreadExecutor();
        commitLogExecutor.submit(() -> {
            try {
                commitLogProcessor.initialize();
                commitLogProcessor.start();
            } catch(Exception e) {
                log.error("commitLogProcessor error:", e);
            }
        });

        ExecutorService commitLogReaderExecutor = Executors.newSingleThreadExecutor();
        commitLogReaderExecutor.submit(() -> {
            try {
                // continuously read commitlogs
                commitLogReaderProcessor.initialize();
                commitLogReaderProcessor.start();
            } catch(Exception e) {
                log.error("commitLogReaderProcessor error:", e);
            }
        });

        log.info("CDC producer agent started");
    }
}
