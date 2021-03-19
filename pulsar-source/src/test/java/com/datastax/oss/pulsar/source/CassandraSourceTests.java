/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastax.oss.pulsar.source;

import net.andreinc.mockneat.MockNeat;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

public class CassandraSourceTests {

    protected static MockNeat mockNeat;

    @Mock
    protected Record<byte[]> mockRecord;

    @Mock
    protected SourceContext mockSourceContext;
    protected Map<String, Object> map;
    protected CassandraSource source;

    @BeforeClass
    public static final void init() {
        mockNeat = MockNeat.threadLocal();
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {
        map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://localhost:9200");
        source = new CassandraSource();

        mockRecord = mock(Record.class);
        mockSourceContext = mock(SourceContext.class);

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            long sequenceCounter = 0;
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
               return Optional.of( "key-" + sequenceCounter++);
            }});

        when(mockRecord.getValue()).thenAnswer(new Answer<byte[]>() {
            public byte[] answer(InvocationOnMock invocation) throws Throwable {
                 return null;
            }});
    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown() throws Exception {
        source.close();
    }

    @Test(enabled = false, expectedExceptions = IllegalArgumentException.class)
    public final void invalidIndexNameTest() throws Exception {
        map.put("indexName", "myIndex");
        source.open(map, mockSourceContext);
    }

    @Test(enabled = false)
    public final void createIndexTest() throws Exception {
        map.put("indexName", "test-index");
        source.open(map, mockSourceContext);
    }

    @Test(enabled = false)
    public final void singleRecordTest() throws Exception {
        map.put("indexName", "test-index");
        source.open(map, mockSourceContext);
        read(1);
        verify(mockRecord, times(1)).ack();
    }

    @Test(enabled = false)
    public final void read100Test() throws Exception {
        map.put("indexName", "test-index");
        source.open(map, mockSourceContext);
        read(100);
        verify(mockRecord, times(100)).ack();
    }

    protected final void read(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            source.read();
        }
    }
}
