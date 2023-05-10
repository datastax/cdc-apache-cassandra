/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.cdc.agent;

import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

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
            startCdcAgent(agentArgs);
        }
    }

    static void startCdcAgent(String agentArgs) throws Exception {
        String agentVersion = Agent.class.getPackage().getImplementationVersion();
        log.info("Starting CDC agent v{}, cdc_raw_directory={}", agentVersion, DatabaseDescriptor.getCDCLogLocation());
        AgentConfig config = AgentConfig.create(AgentConfig.Platform.PULSAR, agentArgs);

        SegmentOffsetFileWriter segmentOffsetFileWriter = new SegmentOffsetFileWriter(config.cdcWorkingDir);
        segmentOffsetFileWriter.loadOffsets();

        PulsarMutationSender pulsarMutationSender = new PulsarMutationSender(config);
        CommitLogTransfer commitLogTransfer = new BlackHoleCommitLogTransfer(config);
        CommitLogReaderServiceImpl commitLogReaderService = new CommitLogReaderServiceImpl(config, pulsarMutationSender, segmentOffsetFileWriter, commitLogTransfer);
        CommitLogProcessor commitLogProcessor = new CommitLogProcessor(DatabaseDescriptor.getCDCLogLocation().getAbsolutePath(), config, commitLogTransfer, segmentOffsetFileWriter, commitLogReaderService, true);

        commitLogReaderService.initialize();

        // detect commitlogs file and submit new/modified files to the commitLogReader
        ExecutorService commitLogExecutor = Executors.newSingleThreadExecutor();
        commitLogExecutor.submit(() -> {
            try {
                do {
                    // wait to initialize the hostID before starting
                    Thread.sleep(1000);
                } while(StorageService.instance.getLocalHostUUID() == null);

                commitLogProcessor.initialize();
                commitLogProcessor.start();
            } catch(Exception e) {
                log.error("commitLogProcessor error:", e);
            }
        });

        ExecutorService commitLogServiceExecutor = Executors.newSingleThreadExecutor();
        commitLogServiceExecutor.submit(commitLogReaderService);

        log.info("CDC agent started");
    }
}
