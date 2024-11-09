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

package org.komamitsu.fluency.fluentd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.EventTime;
import org.msgpack.jackson.dataformat.MessagePackFactory;

class EventTimeTest {
  @Test
  void instantiation() {
    {
      long now = System.currentTimeMillis();
      EventTime eventTime = EventTime.fromEpoch(now / 1000);
      assertThat(eventTime.getSeconds()).isEqualTo(now / 1000);
      assertThat(eventTime.getNanoseconds()).isEqualTo(0L);
    }

    {
      long now = System.currentTimeMillis();
      EventTime eventTime = EventTime.fromEpoch(now / 1000, 999999999L);
      assertThat(eventTime.getSeconds()).isEqualTo(now / 1000);
      assertThat(eventTime.getNanoseconds()).isEqualTo(999999999L);
    }

    {
      long now = System.currentTimeMillis();
      EventTime eventTime = EventTime.fromEpochMilli(now);
      assertThat(eventTime.getSeconds()).isEqualTo(now / 1000);
      assertThat(eventTime.getNanoseconds()).isEqualTo(now % 1000 * 1000000);
    }

    {
      EventTime eventTime = EventTime.fromEpoch(0xFFFFFFFFL, 0xFFFFFFFFL);
      assertThat(eventTime.getSeconds()).isEqualTo(0xFFFFFFFFL);
      assertThat(eventTime.getNanoseconds()).isEqualTo(0xFFFFFFFFL);
    }
  }

  @Test
  void instantiationWithTooLargeSeconds() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          EventTime.fromEpoch(0x100000000L, 0L);
        });
  }

  @Test
  void instantiationWithTooLargeNanoSeconds() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          EventTime.fromEpoch(0L, 0x100000000L);
        });
  }

  @Test
  void serialize() throws JsonProcessingException {
    {
      long now = System.currentTimeMillis();
      EventTime eventTime = EventTime.fromEpoch(now / 1000, 999999999);
      ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
      byte[] bytes = objectMapper.writeValueAsBytes(eventTime);
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xD7);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0x00);
      assertThat(byteBuffer.getInt()).isEqualTo((int) (now / 1000));
      assertThat(byteBuffer.getInt()).isEqualTo(999999999);
    }

    {
      EventTime eventTime = EventTime.fromEpoch(0xFFEEDDCCL, 0xFEDCBA98L);
      ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
      byte[] bytes = objectMapper.writeValueAsBytes(eventTime);
      ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xD7);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0x00);

      assertThat(byteBuffer.get()).isEqualTo((byte) 0xFF);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xEE);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xDD);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xCC);

      assertThat(byteBuffer.get()).isEqualTo((byte) 0xFE);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xDC);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0xBA);
      assertThat(byteBuffer.get()).isEqualTo((byte) 0x98);
    }
  }
}
