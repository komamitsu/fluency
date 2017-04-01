package org.komamitsu.fluency;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.msgpack.jackson.dataformat.MessagePackExtensionType;
import org.msgpack.jackson.dataformat.MessagePackGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@JsonSerialize(using = EventTime.Serilizer.class)
public class EventTime
{
    private final long seconds;
    private final long nanoSeconds;

    public static EventTime fromEpoch(long epochSeconds)
    {
        return new EventTime(epochSeconds, 0);
    }

    public static EventTime fromEpoch(long epochSeconds, long nanoSeconds)
    {
        return new EventTime(epochSeconds, nanoSeconds);
    }

    public static EventTime fromEpochMilli(long epochMilliSecond)
    {
        return new EventTime(epochMilliSecond/ 1000, 0);
    }

    public EventTime(long seconds, long nanoSeconds)
    {
        this.seconds = seconds;
        this.nanoSeconds = nanoSeconds;
    }

    public long getSeconds()
    {
        return seconds;
    }

    public long getNanoSeconds()
    {
        return nanoSeconds;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventTime)) {
            return false;
        }

        EventTime eventTime = (EventTime) o;

        if (seconds != eventTime.seconds) {
            return false;
        }
        return nanoSeconds == eventTime.nanoSeconds;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (seconds ^ (seconds >>> 32));
        result = 31 * result + (int) (nanoSeconds ^ (nanoSeconds >>> 32));
        return result;
    }

    public static class Serilizer
            extends StdSerializer<EventTime>
    {
        public Serilizer()
        {
            super(EventTime.class);
        }

        protected Serilizer(Class<EventTime> t)
        {
            super(t);
        }

        @Override
        public void serialize(EventTime value, JsonGenerator gen, SerializerProvider provider)
                throws IOException
        {
            if (! (gen instanceof MessagePackGenerator)) {
                throw new IllegalStateException("" +
                        "This class should be serialized by MessagePackGenerator, but `gen` is " + gen.getClass());
            }

            MessagePackGenerator messagePackGenerator = (MessagePackGenerator) gen;

            // https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format
            //
            // +-------+----+----+----+----+----+----+----+----+----+
            // |     1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 |
            // +-------+----+----+----+----+----+----+----+----+----+
            // |    D7 | 00 | seconds from epoch|     nanosecond    |
            // +-------+----+----+----+----+----+----+----+----+----+
            // |fixext8|type| 32bits integer BE | 32bits integer BE |
            // +-------+----+----+----+----+----+----+----+----+----+
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.putInt((int) value.seconds).putInt((int) value.nanoSeconds);
            messagePackGenerator.writeExtensionType(new MessagePackExtensionType((byte)0x0, buffer.array()));
        }
    }
}
