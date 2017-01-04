package org.komamitsu.fluency.util;

import org.junit.Test;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class EventTimeTest {

    @Test
    public void testTimeMillis() {

        long fixedTimeMillis = 1483457616859L;

        EventTime time = EventTime.fromMillis(fixedTimeMillis);

        assertEquals(1483457616, time.seconds);
        assertEquals(859000000, time.nanoseconds);
    }

    @Test
    public void testTimestamp() {

        int fixedTimestamp = 1483457616;

        EventTime time = EventTime.fromTimestamp(fixedTimestamp);

        assertEquals(1483457616, time.seconds);
        assertEquals(0, time.nanoseconds);
    }

    // test if the message format is according to spec and that packing/unpacking is working correctly
    @Test
    public void testPacking() {

        long fixedTimeMillis = 1483457616859L;

        EventTime time = EventTime.fromMillis(fixedTimeMillis);

        Exception ex = null;
        byte[] packed;

        try {
            packed = time.pack();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packed);

            /*
               +-------+----+----+----+----+----+----+----+----+----+
               |     1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 |
               +-------+----+----+----+----+----+----+----+----+----+
               |    D7 | 00 | second from epoch |     nanosecond    |
               +-------+----+----+----+----+----+----+----+----+----+
               |fixext8|type| 32bits integer BE | 32bits integer BE |
               +-------+----+----+----+----+----+----+----+----+----+
             */
            assertEquals(MessageFormat.FIXEXT8, unpacker.getNextFormat());
            assertEquals(0x0, unpacker.unpackExtensionTypeHeader().getType());

            assertEquals(time.seconds, unpacker.unpackInt());
            assertEquals(time.nanoseconds, unpacker.unpackInt());

            assertEquals(false, unpacker.hasNext());

            assertEquals(time, EventTime.unpack(packed));

        } catch (IOException e) {
            ex = e;
        }

        assertEquals(null, ex);
    }

}
