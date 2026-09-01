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
package com.datastax.oss.kafka.source.converters;

import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * Converters help to change the format of data from one format into another format.
 * Converters are decoupled from connectors to allow reuse of converters between connectors naturally.
 */
public interface Converter<V, T> {

    /**
     * Convert a Cassandra row to the connector representation.
     */
    V toConnectData(Row r);

    /**
     * Decode an Avro-encoded primary key record into the connector representation.
     */
    T fromConnectData(GenericRecord value) throws IOException;
}
