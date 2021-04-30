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

import java.io.IOException;

/**
 * Standard java.util.Function does not allow to throw checked exception from the apply call To make
 * it explicit we need a CheckedFunction interface that declares that it throws IOException We are
 * specifying IOException instead of Exception to give caller of that function more information
 * about inner behaviour of apply(). If in the future someone wish to make it more generic by
 * throwing Exception will need to change callers to catch Exception instead of IOException.
 */
@FunctionalInterface
public interface CheckedFunction<T, R> {
  R apply(T t) throws IOException;
}
