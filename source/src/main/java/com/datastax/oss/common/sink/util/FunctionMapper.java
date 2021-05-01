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
package com.datastax.oss.common.sink.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class FunctionMapper {
  private static final CqlIdentifier NOW_FUNCTION = CqlIdentifier.fromInternal("now()");
  public static final Set<CqlIdentifier> SUPPORTED_FUNCTIONS_IN_MAPPING =
      ImmutableSet.of(NOW_FUNCTION);

  private static final Map<CqlIdentifier, GenericType<?>> FUNCTION_TYPES =
      ImmutableMap.of(NOW_FUNCTION, GenericType.UUID);

  private static final Map<CqlIdentifier, Supplier<?>> FUNCTION_VALUE_PROVIDER =
      ImmutableMap.of(NOW_FUNCTION, Uuids::timeBased);

  @Nullable
  public static GenericType<?> typeForFunction(String function) {
    return FUNCTION_TYPES.get(CqlIdentifier.fromInternal(function));
  }

  public static Object valueForFunction(String function) {
    return FUNCTION_VALUE_PROVIDER.get(CqlIdentifier.fromInternal(function)).get();
  }
}
