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
