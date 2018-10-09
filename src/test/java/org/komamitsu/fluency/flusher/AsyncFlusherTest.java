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

package org.komamitsu.fluency.flusher;

import org.junit.Test;
import org.komamitsu.fluency.buffer.TestableBuffer;
import org.komamitsu.fluency.sender.MockTCPSender;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AsyncFlusherTest
{
    @Test
    public void testAsyncFlusher()
            throws IOException, InterruptedException
    {
        TestableBuffer buffer = new TestableBuffer.Config().createInstance();
        MockTCPSender sender = new MockTCPSender(24225);
        AsyncFlusher.Config config = new AsyncFlusher.Config();
        config.setFlushIntervalMillis(500);
        Flusher flusher = config.createInstance(buffer, sender);
        assertEquals(0, buffer.getFlushCount().get());

        flusher.onUpdate();
        assertEquals(0, buffer.getFlushCount().get());

        flusher.flush();
        TimeUnit.MILLISECONDS.sleep(50);
        assertEquals(0, buffer.getFlushCount().get());
        assertEquals(1, buffer.getForceFlushCount().get());

        TimeUnit.SECONDS.sleep(1);
        int flushCount = buffer.getFlushCount().get();
        assertTrue(1 <= flushCount && flushCount <= 3);
        int forceFlushCount = buffer.getForceFlushCount().get();
        assertEquals(1, forceFlushCount);

        assertEquals(0, buffer.getCloseCount().get());
        flusher.close();
        assertEquals(1, buffer.getCloseCount().get());
        assertThat(buffer.getFlushCount().get(), is(greaterThanOrEqualTo(2)));
        assertThat(buffer.getFlushCount().get(), is(lessThanOrEqualTo(3)));
        assertThat(buffer.getForceFlushCount().get(), is(greaterThanOrEqualTo(2)));
        assertThat(buffer.getForceFlushCount().get(), is(lessThanOrEqualTo(3)));
    }
}