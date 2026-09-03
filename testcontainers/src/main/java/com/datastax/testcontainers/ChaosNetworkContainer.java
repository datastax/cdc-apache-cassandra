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
package com.datastax.testcontainers;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// see https://github.com/alexei-led/pumba
@Slf4j
public class ChaosNetworkContainer<SELF extends ChaosNetworkContainer<SELF>> extends GenericContainer<SELF> {

    public static final String PUMBA_IMAGE = Optional.ofNullable(System.getenv("PUMBA_IMAGE"))
            .orElse("gaiaadm/pumba:latest");

    public static final String PUMBA_TC_IMAGE = Optional.ofNullable(System.getenv("PUMBA_TC_IMAGE"))
            .orElse("ghcr.io/alexei-led/pumba-debian-nettools");

    private final CountDownLatch chaosFinished = new CountDownLatch(1);

    /** Parsed duration in seconds, used as the await deadline in {@link #stop()}. */
    private final long pauseSeconds;

    public ChaosNetworkContainer(String targetContainer, String pause) {
        super(PUMBA_IMAGE);
        this.pauseSeconds = parsePauseSeconds(pause);
        setCommand("--log-level debug netem --tc-image " + PUMBA_TC_IMAGE + " --duration " + pause + " loss --percent 100 " + targetContainer);
        addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
        // "netem command started" is the debug log pumba emits once the tc
        // sidecar is running and packet loss is active (replaces the old
        // "tc container created" message removed in pumba ~v1.2.x).
        setWaitStrategy(Wait.forLogMessage(".*netem command started.*", 1)
                .withStartupTimeout(Duration.ofSeconds(120)));
        withLogConsumer(o -> {
            final String line = o.getUtf8String();
            if (line != null) {
                // "stopping netem command on timeout/abort" replaces the old
                // "stop netem for container" message removed in pumba ~v1.2.x.
                if (line.contains("stopping netem command on")) {
                    chaosFinished.countDown();
                }
            }
            log.info("pumba> {}", line);
        });
    }


    /**
     * The chaos command must be finished before the container stops.
     * If not, the chaos command will continue forever on the target container.
     * <p>
     * We wait up to {@code pauseSeconds + 30s} for the "stopping netem command on"
     * log line before proceeding.  The extra 30 s covers pumba startup overhead and
     * any log-stream flush delay.  If the deadline is reached without the latch
     * firing (e.g. the final log line was lost when Docker closed the stream),
     * we log a warning and continue — the container will be forcibly stopped by
     * {@link GenericContainer#stop()} which also removes the tc rules.
     */
    @Override
    public void stop() {
        long timeoutSeconds = pauseSeconds + 30;
        log.info("requested stop for ChaosNetworkContainer, awaiting up to {}s for chaos command to finish", timeoutSeconds);
        try {
            boolean finished = chaosFinished.await(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                log.warn("timed out waiting for pumba 'stopping netem command on' log line after {}s; " +
                        "proceeding with container stop (tc rules will be removed by the container shutdown)", timeoutSeconds);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("stopping ChaosNetworkContainer");
        super.stop();
    }

    /**
     * Parses a pumba duration string (e.g. {@code "100s"}, {@code "2m"}) into seconds.
     * Falls back to 300 if the format is unrecognised.
     */
    private static long parsePauseSeconds(String pause) {
        if (pause == null || pause.isEmpty()) return 300;
        try {
            if (pause.endsWith("s")) return Long.parseLong(pause.substring(0, pause.length() - 1));
            if (pause.endsWith("m")) return Long.parseLong(pause.substring(0, pause.length() - 1)) * 60;
            if (pause.endsWith("h")) return Long.parseLong(pause.substring(0, pause.length() - 1)) * 3600;
            return Long.parseLong(pause);
        } catch (NumberFormatException e) {
            return 300;
        }
    }
}
