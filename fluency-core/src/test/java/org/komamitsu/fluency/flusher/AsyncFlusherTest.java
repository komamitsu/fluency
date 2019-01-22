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

import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.JsonRecordFormatter;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.ingester.Ingester;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AsyncFlusherTest
{
    private Ingester ingester;
    private Buffer.Config bufferConfig;
    private AsyncFlusher.Config flusherConfig;

    @Before
    public void setUp()
    {
        ingester = mock(Ingester.class);

        bufferConfig = new Buffer.Config();
        flusherConfig = new AsyncFlusher.Config();
    }

    @Test
    public void testAsyncFlusher()
            throws IOException, InterruptedException
    {
        flusherConfig.setFlushIntervalMillis(500);

        Buffer buffer = spy(new Buffer(bufferConfig, new JsonRecordFormatter()));
        Flusher flusher = new AsyncFlusher(flusherConfig, buffer, ingester);

        verify(buffer, times(0)).flush(eq(ingester), anyBoolean());

        flusher.onUpdate();
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
}