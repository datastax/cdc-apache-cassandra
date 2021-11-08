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
package com.datastax.oss.cdc;

public class Constants {
    /**
     * Writetime message property name.
     */
    public static final String WRITETIME = "writetime";

    /**
     * Commitlog segment id and position message property name.
     */
    public static final String SEGMENT_AND_POSITION = "segpos";

    /**
     * Cassandra partition token property name.
     */
    public static final String TOKEN = "token";
}
