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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.komamitsu.fluency.EventTime;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class EventTimeTest
{
    @Test
    public void instantiation()
    {
        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpoch(now / 1000);
            assertThat(eventTime.getSeconds(), is(now / 1000));
            assertThat(eventTime.getNanoseconds(), is(0L));
        }

        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpoch(now / 1000, 999999999L);
            assertThat(eventTime.getSeconds(), is(now / 1000));
            assertThat(eventTime.getNanoseconds(), is(999999999L));
        }

        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpochMilli(now);
            assertThat(eventTime.getSeconds(), is(now / 1000));
            assertThat(eventTime.getNanoseconds(), Matchers.is(now % 1000 * 1000000));
        }

        {
            EventTime eventTime = EventTime.fromEpoch(0xFFFFFFFFL, 0xFFFFFFFFL);
            assertThat(eventTime.getSeconds(), is(0xFFFFFFFFL));
            assertThat(eventTime.getNanoseconds(), is(0xFFFFFFFFL));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void instantiationWithTooLargeSeconds()
    {
        EventTime.fromEpoch(0x100000000L, 0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void instantiationWithTooLargeNanoSeconds()
    {
        EventTime.fromEpoch(0L, 0x100000000L);
    }

    @Test
    public void serialize()
            throws JsonProcessingException
    {
        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpoch(now / 1000, 999999999);
            ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
            byte[] bytes = objectMapper.writeValueAsBytes(eventTime);
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            assertThat(byteBuffer.get(), is((byte) 0xD7));
            assertThat(byteBuffer.get(), is((byte) 0x00));
            assertThat(byteBuffer.getInt(), is((int) (now / 1000)));
            assertThat(byteBuffer.getInt(), is(999999999));
        }

        {
            EventTime eventTime = EventTime.fromEpoch(0xFFEEDDCCL, 0xFEDCBA98L);
            ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
            byte[] bytes = objectMapper.writeValueAsBytes(eventTime);
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            assertThat(byteBuffer.get(), is((byte) 0xD7));
            assertThat(byteBuffer.get(), is((byte) 0x00));

            assertThat(byteBuffer.get(), is((byte) 0xFF));
            assertThat(byteBuffer.get(), is((byte) 0xEE));
            assertThat(byteBuffer.get(), is((byte) 0xDD));
            assertThat(byteBuffer.get(), is((byte) 0xCC));

            assertThat(byteBuffer.get(), is((byte) 0xFE));
            assertThat(byteBuffer.get(), is((byte) 0xDC));
            assertThat(byteBuffer.get(), is((byte) 0xBA));
            assertThat(byteBuffer.get(), is((byte) 0x98));
        }
    }
}
