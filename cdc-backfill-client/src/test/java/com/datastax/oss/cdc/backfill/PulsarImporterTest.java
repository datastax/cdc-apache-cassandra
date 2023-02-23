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

package com.datastax.oss.cdc.backfill;

import com.datastax.oss.cdc.agent.AbstractMutation;
import com.datastax.oss.cdc.agent.PulsarMutationSender;
import com.datastax.oss.cdc.backfill.factory.PulsarMutationSenderFactory;
import com.datastax.oss.cdc.backfill.factory.SessionFactory;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.csv.CSVConnector;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PulsarImporterTest {
    private Connector connector;
    @Mock
    private TableMetadata tableMetadata;

    @Mock
    private PulsarMutationSenderFactory factory;

    @Mock
    private PulsarMutationSender sender;

    @Captor
    private ArgumentCaptor<AbstractMutation<TableMetadata>> abstractMutationCaptor;
    private PulsarImporter importer;

    @BeforeEach
    public void init() throws Exception {
        MockitoAnnotations.openMocks(this);

        Config connectorConfig = createConnectorConfigs();
        connector = new CSVConnector();
        connector.configure(connectorConfig, true, true);
        connector.init();

        List<ColumnMetadata> pkColumns = new ArrayList<>();
        ColumnIdentifier identifier = new ColumnIdentifier("key", true);
        ColumnMetadata columnMetadata = new ColumnMetadata("ks1", "key", identifier, UTF8Type.instance, 1, ColumnMetadata.Kind.PARTITION_KEY);
        pkColumns.add(columnMetadata);
        Mockito.when(tableMetadata.primaryKeyColumns()).thenReturn(pkColumns);

        Mockito.when(sender.sendMutationAsync(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(factory.newPulsarMutationSender()).thenReturn(sender);

        importer = new PulsarImporter(connector, tableMetadata, factory);
    }

    @AfterEach
    public void cleanup() {
        if (connector != null) {
            try {
                connector.close();
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testImport() {

        // when
        ExitStatus status = importer.importTable();

        // then
        assertEquals(ExitStatus.STATUS_OK, status);
        Mockito.verify(sender, Mockito.times(2)).sendMutationAsync(abstractMutationCaptor.capture());
        List<AbstractMutation<TableMetadata>> pkValues = abstractMutationCaptor.getAllValues();
        assertEquals(2, pkValues.size());
        List<Object> allPkValues = pkValues.stream().flatMap(v-> Arrays.stream(v.getPkValues())).collect(Collectors.toList());
        assertThat(allPkValues, containsInAnyOrder("id3", "id8"));
    }

    private Config createConnectorConfigs() {
        String fileName = "/sample-001.csv";
        Config settings =
                TestConfigUtils.createTestConfig(
                        "dsbulk.connector.csv",
                        "url",
                        url(fileName),
                        "header",
                        true);

        return settings;
    }
    private static String url(String resource) {
        return StringUtils.quoteJson(rawURL(resource));
    }

    private static URL rawURL(String resource) {
        return PulsarImporterTest.class.getResource(resource);
    }
}
