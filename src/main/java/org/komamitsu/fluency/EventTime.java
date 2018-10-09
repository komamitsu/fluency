/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

@JsonSerialize(using = EventTime.Serializer.class)
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

    public static class Serializer
            extends StdSerializer<EventTime>
    {
        public Serializer()
        {
            super(EventTime.class);
        }

        protected Serializer(Class<EventTime> t)
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
