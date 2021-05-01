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
package com.datastax.cassandra.cdc.producer;

import java.time.Duration;

/**
 * This policy determines how frequently the offset is flushed to disk.
 *
 * Periodic means that the offset is flushed to disk periodically according to the offset flush interval.
 * An interval of 1000 ms that the latest offset will only be flushed to disk if the amount time that has
 * passed since the last flush is at least 1000 ms.
 *
 * Always means that the offset if flushed to disk every time a record is processed.
 */
public interface OffsetFlushPolicy {
    boolean shouldFlush(Duration timeSinceLastFlush, long numOfRecordsSinceLastFlush);

    static OffsetFlushPolicy always() {
        return new AlwaysFlushOffsetPolicy();
    }

    static OffsetFlushPolicy periodic(Duration offsetFlushInterval, long maxOffsetFlushSize) {
        return new PeriodicFlushOffsetPolicy(offsetFlushInterval, maxOffsetFlushSize);
    }

    class PeriodicFlushOffsetPolicy implements OffsetFlushPolicy {
        private final Duration offsetFlushInterval;
        private final long maxOffsetFlushSize;

        PeriodicFlushOffsetPolicy(Duration offsetFlushInterval, long maxOffsetFlushSize) {
            this.offsetFlushInterval = offsetFlushInterval;
            this.maxOffsetFlushSize = maxOffsetFlushSize;
        }

        @Override
        public boolean shouldFlush(Duration timeSinceLastFlush, long numOfRecordsSinceLastFlush) {
            return timeSinceLastFlush.compareTo(offsetFlushInterval) >= 0 || numOfRecordsSinceLastFlush >= this.maxOffsetFlushSize;
        }
    }

    class AlwaysFlushOffsetPolicy implements OffsetFlushPolicy {

        @Override
        public boolean shouldFlush(Duration timeSinceLastFlush, long numOfRecordsSinceLastFlush) {
            return true;
        }
    }
}
