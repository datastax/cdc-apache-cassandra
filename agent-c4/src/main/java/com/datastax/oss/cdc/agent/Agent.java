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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Agent {
    public static void premain(String agentArgs, Instrumentation inst) {
        log.info("[Agent] In premain method");
        startAsync(agentArgs, inst);
    }

    public static void agentmain(String agentArgs, Instrumentation inst) {
        log.info("[Agent] In agentmain method");
        startAsync(agentArgs, inst);
    }

    private static void startAsync(String agentArgs, Instrumentation inst) {
        Thread thread = new Thread(() -> {
            try {
                waitForSeedProviderOnClasspath();
                main(agentArgs, inst);
            } catch (Exception e) {
                log.error("error:", e);
                System.exit(-1);
            }
        }, "cdc-agent-init");
        thread.start();
    }

    private static void waitForSeedProviderOnClasspath() throws InterruptedException {
        for (int i = 0; i < 30; i++) {
            try {
                Class.forName("org.apache.cassandra.locator.K8SeedProvider", false, Agent.class.getClassLoader());
                return;
            } catch (ClassNotFoundException e) {
                Thread.sleep(200);
            }
        }
    }

    static void main(String agentArgs, Instrumentation inst) throws Exception {
        daemonInitializationWithRetry();
        if (DatabaseDescriptor.isCDCEnabled() == false) {
            log.error("cdc_enabled=false in your cassandra configuration, CDC agent not started.");
        } else if (DatabaseDescriptor.getCDCLogLocation() == null) {
            log.error("cdc_raw_directory=null in your cassandra configuration, CDC agent not started.");
        } else {
            startCdcAgent(agentArgs);
        }
    }
    private static void daemonInitializationWithRetry() throws InterruptedException {
        int attempts = 0;
        while (true) {
            try {
                DatabaseDescriptor.daemonInitialization();
                return;
            } catch (ConfigurationException e) {
                attempts++;
                if (attempts >= 30) {
                    throw e;
                }
                log.warn("DatabaseDescriptor.daemonInitialization() failed (attempt {}/30), retrying: {}", attempts, e.getMessage());
                Thread.sleep(1000);
            }
        }
    }

    static void startCdcAgent(String agentArgs) throws Exception {
        log.info("Starting CDC agent, cdc_raw_directory={}", DatabaseDescriptor.getCDCLogLocation());

        AgentConfig.Platform platform = AgentConfig.extractPlatform(agentArgs);
        String strippedArgs = AgentConfig.stripParam(agentArgs, "platform");
        AgentConfig config = AgentConfig.create(platform, strippedArgs);

        SegmentOffsetFileWriter segmentOffsetFileWriter = new SegmentOffsetFileWriter(config.cdcWorkingDir);
        segmentOffsetFileWriter.loadOffsets();

        @SuppressWarnings("unchecked")
        MutationSender<TableMetadata> mutationSender = platform == AgentConfig.Platform.KAFKA
                ? (MutationSender<TableMetadata>) new KafkaMutationSender(config)
                : new PulsarMutationSender(config);
        CommitLogTransfer commitLogTransfer = new BlackHoleCommitLogTransfer(config);
        CommitLogReaderServiceImpl commitLogReaderService = new CommitLogReaderServiceImpl(config, mutationSender, segmentOffsetFileWriter, commitLogTransfer);
        CommitLogProcessor commitLogProcessor = new CommitLogProcessor(DatabaseDescriptor.getCDCLogLocation(), config, commitLogTransfer, segmentOffsetFileWriter, commitLogReaderService, true);

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
