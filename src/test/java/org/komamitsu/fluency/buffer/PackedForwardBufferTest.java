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

package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.ingester.fluentdsender.MockTCPSender;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class PackedForwardBufferTest
{
    @Test
    public void testPackedForwardBuffer()
            throws IOException, InterruptedException
    {
        for (Integer loopCount : Arrays.asList(100, 1000, 10000, 200000)) {
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, false, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, true, false, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, false, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, false, false, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, true, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, true, true, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, true, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, false, true, new PackedForwardBuffer.Config().createInstance());
        }
    }

    @Test
    public void testGetAllocatedSize()
            throws IOException
    {
        PackedForwardBuffer buffer = new PackedForwardBuffer.Config().setChunkInitialSize(256 * 1024).createInstance();
        assertThat(buffer.getAllocatedSize(), is(0L));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foo.bar", new Date().getTime(), map);
        }
        assertThat(buffer.getAllocatedSize(), is(256 * 1024L));
    }

    @Test
    public void testGetBufferedDataSize()
            throws IOException, InterruptedException
    {
        PackedForwardBuffer buffer = new PackedForwardBuffer.Config().setChunkInitialSize(256 * 1024).createInstance();
        assertThat(buffer.getBufferedDataSize(), is(0L));

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foo.bar", new Date().getTime(), map);
        }
        assertThat(buffer.getBufferedDataSize(), is(greaterThan(0L)));
        assertThat(buffer.getBufferedDataSize(), is(lessThan(512L)));

        MockTCPSender sender = new MockTCPSender(24224);
        buffer.flush(sender, true);
        assertThat(buffer.getBufferedDataSize(), is(0L));
    }

    @Test
    public void testAppendIfItDoesNotThrowBufferOverflow()
            throws IOException
    {
        PackedForwardBuffer buffer = new PackedForwardBuffer.Config().setChunkInitialSize(64 * 1024).createInstance();

        StringBuilder buf = new StringBuilder();

        for (int i = 0; i < 1024 * 60; i++) {
            buf.append('x');
        }
        String str60kb = buf.toString();

        for (int i = 0; i < 1024 * 40; i++) {
            buf.append('x');
        }
        String str100kb = buf.toString();

        {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("k", str60kb);
            buffer.append("tag0", new Date().getTime(), map);
        }

        {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("k", str100kb);
            buffer.append("tag0", new Date().getTime(), map);
        }
    }
}