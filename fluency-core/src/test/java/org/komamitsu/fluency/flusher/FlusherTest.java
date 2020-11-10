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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    void queueSizeShouldNotExceedLimit()
    {
        flusherConfig.setFlushAttemptIntervalMillis(1000);

        Buffer buffer = new Buffer(bufferConfig, new JsonRecordFormatter());
        Flusher flusher = new Flusher(flusherConfig, buffer, ingester);

        // Just for checking Flusher#queuedSize
        flusher.flush();
        assertEquals(1, flusher.queuedSize());

        for (int i = 0; i < 10000; i++) {
            flusher.flush();
            assertTrue(flusher.queuedSize() < 20);
        }
    }
}
