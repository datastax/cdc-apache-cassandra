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

package com.datastax.oss.cdc.backfill.exporter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
public class ExportedColumn {

    public final ColumnMetadata col;
    public final boolean pk;
    public final CqlIdentifier writetime;
    public final CqlIdentifier ttl;

    public ExportedColumn(
            ColumnMetadata col, boolean pk, CqlIdentifier writetime, CqlIdentifier ttl) {
        this.col = col;
        this.pk = pk;
        this.writetime = writetime;
        this.ttl = ttl;
    }
}
