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

package com.datastax.oss.cdc.backfill.util;

import com.datastax.oss.cdc.backfill.BackFillSettings;
import com.datastax.oss.cdc.backfill.ClusterInfo;
import com.datastax.oss.cdc.backfill.Credentials;
import com.datastax.oss.cdc.backfill.ExportedColumn;
import com.datastax.oss.cdc.backfill.ExportedTable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.util.ArrayList;
import java.util.List;

public class TableUtils {


}
