/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastax.oss.pulsar.source;

import com.datastax.cassandra.cdc.MutationValue;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.pulsar.source.converters.JsonConverter;
import com.google.common.collect.Lists;
import net.andreinc.mockneat.MockNeat;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;

import org.apache.pulsar.io.core.SourceContext;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testcontainers.cassandra.CassandraContainer;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CassandraSourceTests {

    public final static String CASSANDRA_IMAGE = "cassandra:3.11.2";

    static MockNeat mockNeat;

    static CassandraContainer cassandraContainer = new CassandraContainer(CASSANDRA_IMAGE);

    @Mock
    SourceContext mockSourceContext;

    @Mock
    protected ConsumerBuilder mockConsumerBuilder;

    @Mock
    protected Consumer mockConsumer;

    @Mock
    protected Message<KeyValue<Object, MutationValue>> mockMessage;

    protected CassandraSource source;

    protected Map<String, Object> map;

    @BeforeAll
    public static final void initBeforeClass() throws InterruptedException {
        mockNeat = MockNeat.threadLocal();
        cassandraContainer.start();
        Thread.sleep(15000);
        try(CqlSession cqlSession = cassandraContainer.getCqlSession()) {
            cqlSession.execute("CREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            cqlSession.execute("CREATE TABLE IF NOT EXISTS ks1.table1 (id text PRIMARY KEY, a int)");
            cqlSession.execute("INSERT INTO ks1.table1 (id, a) VALUES('1',1)");
        }
    }

    @AfterAll
    public static void closeAfterClass() {
        cassandraContainer.stop();
        cassandraContainer.close();
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    public final void setUp() throws Exception {

        map = new HashMap<String, Object>();
        map.put("contactPoints", cassandraContainer.getHost() + ":" + cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT));
        map.put("localDc", "datacenter1");
        map.put("keyspace", "ks1");
        map.put("table", "table1");
        map.put("keyConverter", "com.datastax.oss.pulsar.source.converters.JsonConverter");
        map.put("valueConverter", "com.datastax.oss.pulsar.source.converters.AvroConverter");
        map.put("dirtyTopicPrefix", "dirty-");
        source = new CassandraSource();
        mockSourceContext = mock(SourceContext.class);
        mockConsumerBuilder = mock(ConsumerBuilder.class);
        mockConsumer = mock(Consumer.class);
        mockMessage = mock(Message.class);

        ColumnMetadata idColumn = new ColumnMetadataImpl(
                CqlIdentifier.fromInternal("ks1"),
                CqlIdentifier.fromInternal("table1"),
                CqlIdentifier.fromInternal("id"),
                new PrimitiveType(ProtocolConstants.DataType.ASCII),
                false);

        try(CqlSession session = cassandraContainer.getCqlSession()) {
            KeyspaceMetadata ksm = session.getMetadata().getKeyspace("ks1").get();
            TableMetadata tm = ksm.getTable("table1").get();
            JsonConverter jsonConverter = new JsonConverter(ksm, tm, Lists.newArrayList(idColumn));
            GenericRecord genericRecord = new GenericJsonSchema(jsonConverter.schemaInfo).newRecordBuilder()
                    .set("id", "1")
                    .build();
            MutationValue mutationValue = new MutationValue("digest1", null, null);

            when(mockSourceContext.newConsumerBuilder(source.dirtySchema)).thenReturn(mockConsumerBuilder);

            when(mockConsumerBuilder.topic("dirty-ks1.table1")).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.consumerName("CDC Consumer")).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.autoUpdatePartitions(true)).thenReturn(mockConsumerBuilder);
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);

            when(mockConsumer.receive()).thenReturn(mockMessage);

            when(mockMessage.getValue()).then(new Answer<KeyValue<GenericRecord, MutationValue>>() {
                @Override
                public KeyValue<GenericRecord, MutationValue> answer(InvocationOnMock invocationOnMock) throws Throwable {
                    return new KeyValue(genericRecord, mutationValue);
                }
            });
            when(mockMessage.getKey()).then(new Answer<String>() {
                @Override
                public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                    return "1";
                }
            });
        }
    }

    @AfterEach
    public final void tearDown() throws Exception {
        source.close();
    }

    @Test
    public final void singleRecordTest() throws Exception {
        source.open(map, mockSourceContext);
        Record<GenericRecord> record = source.read();
        KeyValue<GenericRecord, GenericRecord> keyValue = (KeyValue<GenericRecord, GenericRecord>) record.getValue();
        GenericRecord key = keyValue.getKey();
        GenericRecord val = keyValue.getValue();
        assertEquals("1", key.getField("id"));
        assertEquals(1, val.getField("a"));
    }
}
