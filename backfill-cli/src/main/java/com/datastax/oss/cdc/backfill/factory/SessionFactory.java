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
package com.datastax.oss.cdc.backfill.factory;

import com.datastax.oss.cdc.backfill.exporter.ClusterInfo;
import com.datastax.oss.cdc.backfill.exporter.Credentials;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionFactory.class);

    public CqlSession newSession(ClusterInfo clusterInfo, Credentials credentials) {
        String clusterName = clusterInfo.isOrigin() ? "origin" : "target";
        try {
            LOGGER.info("Contacting {} cluster...", clusterName);
            CqlSession session = createSessionBuilder(clusterInfo, credentials).build();
            LOGGER.info("Successfully contacted {} cluster", clusterName);
            return session;
        } catch (Exception e) {
            throw new IllegalStateException("Could not contact " + clusterName + " cluster", e);
        }
    }

    private static CqlSessionBuilder createSessionBuilder(
            ClusterInfo clusterInfo, Credentials credentials) {
        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
                DriverConfigLoader.programmaticBuilder()
                        .withString(
                                DefaultDriverOption.SESSION_NAME, clusterInfo.isOrigin() ? "origin" : "target")
                        .withString(
                                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "DcInferringLoadBalancingPolicy");
        if (clusterInfo.getProtocolVersion() != null) {
            configLoaderBuilder =
                    configLoaderBuilder.withString(
                            DefaultDriverOption.PROTOCOL_VERSION, clusterInfo.getProtocolVersion());
        }
        DriverConfigLoader configLoader = configLoaderBuilder.build();
        CqlSessionBuilder sessionBuilder = CqlSession.builder().withConfigLoader(configLoader);
        if (clusterInfo.isAstra()) {
            sessionBuilder.withCloudSecureConnectBundle(clusterInfo.getBundle());
        } else {
            List<InetSocketAddress> contactPoints = clusterInfo.getContactPoints();
            sessionBuilder.addContactPoints(contactPoints);
            // limit connectivity to just the contact points to limit network I/O
            // TODO: Understand better the node filter and update to NodeDistanceEvaluator instead:
            // https://docs.datastax.com/en/developer/java-driver/4.11/upgrade_guide/
            //sessionBuilder.withNodeFilter(
            //        node -> {
            //            SocketAddress address = node.getEndPoint().resolve();
            //            return address instanceof InetSocketAddress && contactPoints.contains(address);
            //        });
        }
        if (credentials != null) {
            sessionBuilder.withAuthCredentials(
                    credentials.getUsername(), String.valueOf(credentials.getPassword()));
        }
        return sessionBuilder;
    }
}

