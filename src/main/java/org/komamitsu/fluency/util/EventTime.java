package org.komamitsu.fluency.util;

import org.msgpack.core.*;

import java.io.IOException;
import java.io.Serializable;

/**
 * Implementation of Fluentd EventTime
 * This is susceptible to integer overruns and also affected by the 2038 problem, but so is their spec
 * They use ints internally, so must we
 */
public class EventTime implements Serializable {

    int seconds;
    int nanoseconds;

    public EventTime(int seconds, int nanoseconds) {
        this.seconds = seconds;
        this.nanoseconds = nanoseconds;
    }

    public static EventTime fromTimestamp(int timestamp) {
        return new EventTime(timestamp, 0);
    }

    public static EventTime fromTimestamp(long timestamp) {
        // the lossy cast is unfortunately required due to their spec
        return new EventTime((int)timestamp, 0);
    }

    public static EventTime fromMillis(long timeInMillis) {
        return new EventTime((int)(timeInMillis / 1000), (int)(timeInMillis % 1000 * 1000000));
    }

    public byte[] pack() throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        // https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1 => EventTime Ext Format
        /*
           +-------+----+----+----+----+----+----+----+----+----+
           |     1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 |
           +-------+----+----+----+----+----+----+----+----+----+
           |    D7 | 00 | second from epoch |     nanosecond    |
           +-------+----+----+----+----+----+----+----+----+----+
           |fixext8|type| 32bits integer BE | 32bits integer BE |
           +-------+----+----+----+----+----+----+----+----+----+
         */
        packer.packExtensionTypeHeader((byte) 0, 8);
        packer.packInt((int)this.seconds);
        packer.packInt((int)this.nanoseconds);

        return packer.toByteArray();
    }

    public static EventTime unpack(byte[] message) {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(message);

        int seconds;
        int nanoseconds;

        try {
            if (!unpacker.getNextFormat().equals(MessageFormat.FIXEXT8)) return null;
            if (unpacker.unpackExtensionTypeHeader().getType() != 0x0)   return null;

            seconds = unpacker.unpackInt();
            nanoseconds = unpacker.unpackInt();
        } catch (IOException e) {
            return null;
        }

        return new EventTime(seconds, nanoseconds);
    }

    @Override
    public String toString() {
        return "EventTime{" +
                "seconds=" + seconds +
                ", nanoseconds=" + nanoseconds +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventTime eventTime = (EventTime) o;

        if (seconds != eventTime.seconds) return false;
        return nanoseconds == eventTime.nanoseconds;
    }

    @Override
    public int hashCode() {
        int result = seconds;
        result = 31 * result + nanoseconds;
        return result;
    }

}
