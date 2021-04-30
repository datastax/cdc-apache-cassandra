/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.common.sink.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/** Utility class to house useful methods and constants that the rest of the application may use. */
public class SinkUtil {
  public static final String TIMESTAMP_VARNAME = "message_internal_timestamp";
  public static final String TTL_VARNAME = "message_internal_ttl";
  public static final CqlIdentifier TTL_VARNAME_CQL_IDENTIFIER =
      CqlIdentifier.fromInternal(TTL_VARNAME);
  public static final CqlIdentifier TIMESTAMP_VARNAME_CQL_IDENTIFIER =
      CqlIdentifier.fromInternal(TIMESTAMP_VARNAME);

  public static final String NAME_OPT = "name";

  /** This is a utility class and should never be instantiated. */
  private SinkUtil() {}

  public static boolean isTtlMappingColumn(CqlIdentifier col) {
    return col.equals(TTL_VARNAME_CQL_IDENTIFIER);
  }

  public static boolean isTimestampMappingColumn(CqlIdentifier col) {
    return col.equals(TIMESTAMP_VARNAME_CQL_IDENTIFIER);
  }
}
