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

import static org.junit.Assert.*;

public class SyncFlusherTest
{
    @Test
    public void testSyncFlusher()
            throws IOException, InterruptedException
    {
        TestableBuffer buffer = new TestableBuffer.Config().createInstance();
        MockTCPSender sender = new MockTCPSender(24225);
        SyncFlusher.Config config = new SyncFlusher.Config();
        assertEquals(600, config.getFlushIntervalMillis());
        Flusher flusher = config.createInstance(buffer, sender);

        flusher.flush();
        flusher.flush();
        flusher.flush();
        assertEquals(0, buffer.getFlushCount().get());
        assertEquals(3, buffer.getForceFlushCount().get());

        flusher.onUpdate();
        flusher.onUpdate();
        flusher.onUpdate();
        assertEquals(0, buffer.getFlushCount().get());
        assertEquals(3, buffer.getForceFlushCount().get());

        TimeUnit.SECONDS.sleep(1);
        flusher.onUpdate();
        assertEquals(1, buffer.getFlushCount().get());
        assertEquals(3, buffer.getForceFlushCount().get());

        assertEquals(0, buffer.getCloseCount().get());
        flusher.close();
        assertEquals(1, buffer.getCloseCount().get());
        assertEquals(1, buffer.getFlushCount().get());
        assertEquals(3 + 1, buffer.getForceFlushCount().get());
    }
}