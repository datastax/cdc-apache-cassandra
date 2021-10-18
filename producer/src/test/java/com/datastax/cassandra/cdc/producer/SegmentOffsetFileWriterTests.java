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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SegmentOffsetFileWriterTests {

    @Test
    public void testSegmentOffsetFileWriter() throws IOException {
        SegmentOffsetFileWriter segmentOffsetFileWriter = new SegmentOffsetFileWriter("producer/build/cdc");
        long seg1 = 12345671L;
        long seg2 = 12345672L;
        segmentOffsetFileWriter.position(Optional.empty(), seg1, 3);
        segmentOffsetFileWriter.position(Optional.empty(), seg2, 4);
        segmentOffsetFileWriter.flush(Optional.empty(), seg1);
        segmentOffsetFileWriter.position(Optional.empty(), seg1, 0);
        segmentOffsetFileWriter.loadOffset(seg1);
        assertEquals(3, segmentOffsetFileWriter.position(Optional.empty(), seg1));
        assertEquals(4, segmentOffsetFileWriter.position(Optional.empty(), seg2));

        segmentOffsetFileWriter.close();
        SegmentOffsetFileWriter segmentOffsetFileWriter2 = new SegmentOffsetFileWriter("cdc");
        segmentOffsetFileWriter2.loadOffsets();
        assertEquals(3, segmentOffsetFileWriter.position(Optional.empty(), seg1));
        assertEquals(4, segmentOffsetFileWriter.position(Optional.empty(), seg2));
    }
}
