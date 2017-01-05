package org.komamitsu.fluency.util;

import org.msgpack.jackson.dataformat.MessagePackExtensionType;

import java.io.Serializable;
import java.nio.ByteBuffer;

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

    public MessagePackExtensionType pack() {
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
        return new MessagePackExtensionType((byte)0x0, ByteBuffer.allocate(8).putInt(this.seconds).putInt(this.nanoseconds).array());
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
