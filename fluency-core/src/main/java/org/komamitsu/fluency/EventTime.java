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
import java.io.IOException;
import java.nio.ByteBuffer;
import org.msgpack.jackson.dataformat.MessagePackExtensionType;
import org.msgpack.jackson.dataformat.MessagePackGenerator;

@JsonSerialize(using = EventTime.Serializer.class)
public class EventTime {
  private final long seconds;
  private final long nanoseconds;

  /**
   * Constructs an <code>EventTime</code>.
   *
   * @param seconds the epoch seconds. This should be a 32-bit value.
   * @param nanoseconds the nanoseconds. This should be a 32-bit value.
   */
  public EventTime(long seconds, long nanoseconds) {
    if (seconds >> 32 != 0) {
      throw new IllegalArgumentException("`seconds` should be a 32-bit value");
    }

    if (nanoseconds >> 32 != 0) {
      throw new IllegalArgumentException("`nanoseconds` should be a 32-bit value");
    }

    this.seconds = seconds;
    this.nanoseconds = nanoseconds;
  }

  /**
   * Constructs an <code>EventTime</code>.
   *
   * @param epochSeconds the epoch seconds. This should be a 32-bit value.
   */
  public static EventTime fromEpoch(long epochSeconds) {
    return new EventTime(epochSeconds, 0);
  }

  /**
   * Constructs an <code>EventTime</code>.
   *
   * @param epochSeconds the epoch seconds. This should be a 32-bit value.
   * @param nanoseconds the nanoseconds. This should be a 32-bit value.
   */
  public static EventTime fromEpoch(long epochSeconds, long nanoseconds) {
    return new EventTime(epochSeconds, nanoseconds);
  }

  /**
   * Constructs an <code>EventTime</code>.
   *
   * @param epochMillisecond the epoch milli seconds. This should be a 32-bit value.
   */
  public static EventTime fromEpochMilli(long epochMillisecond) {
    return new EventTime(epochMillisecond / 1000, (epochMillisecond % 1000) * 1000000);
  }

  public long getSeconds() {
    return seconds;
  }

  public long getNanoseconds() {
    return nanoseconds;
  }

  /** @deprecated As of release 1.9, replaced by {@link #getNanoseconds()} */
  @Deprecated
  public long getNanoSeconds() {
    return nanoseconds;
  }

  @Override
  public boolean equals(Object o) {
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
    return nanoseconds == eventTime.nanoseconds;
  }

  @Override
  public int hashCode() {
    int result = (int) (seconds ^ (seconds >>> 32));
    result = 31 * result + (int) (nanoseconds ^ (nanoseconds >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "EventTime{" + "seconds=" + seconds + ", nanoseconds=" + nanoseconds + '}';
  }

  public static class Serializer extends StdSerializer<EventTime> {
    public Serializer() {
      super(EventTime.class);
    }

    protected Serializer(Class<EventTime> t) {
      super(t);
    }

    @Override
    public void serialize(EventTime value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      if (!(gen instanceof MessagePackGenerator)) {
        throw new IllegalStateException(
            ""
                + "This class should be serialized by MessagePackGenerator, but `gen` is "
                + gen.getClass());
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
      buffer.putInt((int) value.seconds).putInt((int) value.nanoseconds);
      messagePackGenerator.writeExtensionType(
          new MessagePackExtensionType((byte) 0x0, buffer.array()));
    }
  }
}
