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
    private final int seconds;
    private final int nanoSeconds;

    public static EventTime fromEpoch(int epochSeconds)
    {
        return new EventTime(epochSeconds, 0);
    }

    public static EventTime fromEpoch(int epochSeconds, int nanoSeconds)
    {
        return new EventTime(epochSeconds, nanoSeconds);
    }

    public static EventTime fromEpochMilli(long epochMilliSecond)
    {
        return new EventTime((int) (epochMilliSecond/ 1000), (int) ((epochMilliSecond % 1000) * 1000000));
    }

    public EventTime(int seconds, int nanoSeconds)
    {
        this.seconds = seconds;
        this.nanoSeconds = nanoSeconds;
    }

    public int getSeconds()
    {
        return seconds;
    }

    public int getNanoSeconds()
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
        int result = seconds;
        result = 31 * result + nanoSeconds;
        return result;
    }

    @Override
    public String toString()
    {
        return "EventTime{" +
                "seconds=" + seconds +
                ", nanoSeconds=" + nanoSeconds +
                '}';
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
            buffer.putInt(value.seconds).putInt(value.nanoSeconds);
            messagePackGenerator.writeExtensionType(new MessagePackExtensionType((byte)0x0, buffer.array()));
        }
    }
}
