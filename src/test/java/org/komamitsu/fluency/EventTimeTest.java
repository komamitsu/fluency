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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class EventTimeTest
{
    @Test
    public void instantiation()
    {
        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpoch((int) (now / 1000));
            assertThat(eventTime.getSeconds(), is((int) (now / 1000)));
            assertThat(eventTime.getNanoSeconds(), is(0));
        }

        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpoch((int) (now / 1000), 999999999);
            assertThat(eventTime.getSeconds(), is((int) (now / 1000)));
            assertThat(eventTime.getNanoSeconds(), is(999999999));
        }

        {
            long now = System.currentTimeMillis();
            EventTime eventTime = EventTime.fromEpochMilli(now);
            assertThat(eventTime.getSeconds(), is((int) (now / 1000)));
            assertThat(eventTime.getNanoSeconds(), Matchers.is((int) (now % 1000 * 1000000)));
        }
    }

    @Test
    public void serialize()
            throws JsonProcessingException
    {
        long now = System.currentTimeMillis();
        EventTime eventTime = EventTime.fromEpoch((int) (now / 1000), 999999999);
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        byte[] bytes = objectMapper.writeValueAsBytes(eventTime);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        assertThat(byteBuffer.get(), is((byte) 0xD7));
        assertThat(byteBuffer.get(), is((byte) 0x00));
        assertThat(byteBuffer.getInt(), is((int) (now / 1000)));
        assertThat(byteBuffer.getInt(), is(999999999));
    }
}