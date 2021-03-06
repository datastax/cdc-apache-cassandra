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
package com.datastax.oss.cdc.agent;

import java.util.function.Consumer;

/**
 * A variant of {@link Consumer} that can be blocked and interrupted.
 * @param <T> the type of the input to the operation
 * @author Randall Hauch
 */
@FunctionalInterface
public interface BlockingConsumer<T> {

    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws InterruptedException if the calling thread is interrupted while blocking
     */
    void accept(T t) throws InterruptedException;
}
