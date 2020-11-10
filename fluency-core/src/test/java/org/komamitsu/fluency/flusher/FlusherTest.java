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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.JsonRecordFormatter;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.ingester.Ingester;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FlusherTest
{
    private Ingester ingester;
    private Buffer.Config bufferConfig;
    private Flusher.Config flusherConfig;

    @BeforeEach
    void setUp()
    {
        ingester = mock(Ingester.class);

        bufferConfig = new Buffer.Config();
        flusherConfig = new Flusher.Config();
    }

    @Test
    void testAsyncFlusher()
            throws IOException, InterruptedException
    {
        flusherConfig.setFlushAttemptIntervalMillis(500);

        Buffer buffer = spy(new Buffer(bufferConfig, new JsonRecordFormatter()));
        Flusher flusher = new Flusher(flusherConfig, buffer, ingester);

        verify(buffer, times(0)).flush(eq(ingester), anyBoolean());

        verify(buffer, times(0)).flush(eq(ingester), anyBoolean());

        flusher.flush();
        TimeUnit.MILLISECONDS.sleep(50);
        verify(buffer, times(0)).flush(eq(ingester), eq(false));
        verify(buffer, times(1)).flush(eq(ingester), eq(true));

        TimeUnit.SECONDS.sleep(1);
        verify(buffer, atLeast(1)).flush(eq(ingester), eq(false));
        verify(buffer, atMost(3)).flush(eq(ingester), eq(false));
        verify(buffer, times(1)).flush(eq(ingester), eq(true));

        verify(buffer, times(0)).close();

        flusher.close();
        verify(buffer, times(1)).close();

        verify(buffer, atLeast(2)).flush(eq(ingester), eq(false));
        verify(buffer, atMost(3)).flush(eq(ingester), eq(false));

        verify(buffer, atLeast(2)).flush(eq(ingester), eq(true));
        verify(buffer, atMost(3)).flush(eq(ingester), eq(true));
    }

    @Test
    void validateConfig()
    {
        Buffer buffer = spy(new Buffer(bufferConfig, new JsonRecordFormatter()));

        {
            Flusher.Config config = new Flusher.Config();
            config.setFlushAttemptIntervalMillis(19);
            assertThrows(IllegalArgumentException.class, () -> new Flusher(config, buffer, ingester));
        }

        {
            Flusher.Config config = new Flusher.Config();
            config.setFlushAttemptIntervalMillis(2001);
            assertThrows(IllegalArgumentException.class, () -> new Flusher(config, buffer, ingester));
        }

        {
            Flusher.Config config = new Flusher.Config();
            config.setWaitUntilBufferFlushed(0);
            assertThrows(IllegalArgumentException.class, () -> new Flusher(config, buffer, ingester));
        }

        {
            Flusher.Config config = new Flusher.Config();
            config.setWaitUntilTerminated(0);
            assertThrows(IllegalArgumentException.class, () -> new Flusher(config, buffer, ingester));
        }
    }

    @Test
    void queueSizeShouldNotExceedLimit() throws Exception
    {
        flusherConfig.setFlushAttemptIntervalMillis(200);

        Buffer buffer = new Buffer(bufferConfig, new JsonRecordFormatter());
        Flusher flusher = new Flusher(flusherConfig, buffer, ingester);

        BlockingQueue<Boolean> eventQueue = (BlockingQueue<Boolean>) extractValue(flusher, "eventQueue");

        int nonZeroCounts = 0;
        for (int index = 0; index < 10_000; index++) {
            flusher.flush();
            if(!eventQueue.isEmpty()) {
                nonZeroCounts++;
            }
            // The eventQueue will always have less that 16 elements (the default max size)
            assertTrue(eventQueue.size() <= 16);
        }
        // The eventQueue will be non empty at least a couple of times
        assertTrue(nonZeroCounts > 0);

        // Wait for sufficiently long (amount of time > queue poll wait) and the queue should be polled completely to become empty
        Thread.sleep(1000);
        assertEquals(0, eventQueue.size());
    }

    private Object extractValue(Object object, String fieldName) throws Exception
    {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(object);
    }
}
