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
package com.datastax.oss.cdc.converters;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Reflectively instantiates a platform's row/key converter. Every Pulsar and Kafka converter
 * shares the same constructor shape ({@code KeyspaceMetadata, TableMetadata, List<ColumnMetadata>}),
 * so this lookup is platform-free; each caller casts the result to its own {@code Converter} type.
 */
public final class ConverterFactory {

    private ConverterFactory() {
    }

    @SuppressWarnings("unchecked")
    public static <C> C create(Class<?> converterClass, KeyspaceMetadata ksm, TableMetadata tableMetadata, List<ColumnMetadata> columns)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        return (C) converterClass
                .getDeclaredConstructor(KeyspaceMetadata.class, TableMetadata.class, List.class)
                .newInstance(ksm, tableMetadata, columns);
    }

    /**
     * Resolves a config-supplied converter class, falling back to the platform's own default
     * Avro/Json converter when none was configured.
     */
    public static Class<?> resolveConverterClass(Class<?> configuredClass, boolean jsonOutputFormat, Class<?> jsonDefault, Class<?> avroDefault) {
        return configuredClass == null ? (jsonOutputFormat ? jsonDefault : avroDefault) : configuredClass;
    }
}
