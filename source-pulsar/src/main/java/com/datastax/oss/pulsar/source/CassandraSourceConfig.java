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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class CassandraSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "A comma-separated list of cassandra hosts to connect to")
    private String contactPoints;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Cassandra local datacenter")
    private String localDc;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Cassandra keyspace name")
    private String keyspace;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Cassandra table name")
    private String table;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The pulsar events topic name.")
    private String eventsTopicPrefix;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The pulsar events topic subscription name.")
    private String eventsSubscriptionName;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The converter class name used to convert a Cassandra row key to a pulsar IO record.")
    private String keyConverter;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The converter class name used to convert a Cassandra row value to a pulsar IO record.")
    private String valueConverter;

    public static CassandraSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), CassandraSourceConfig.class);
    }

    public static CassandraSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), CassandraSourceConfig.class);
    }
}
