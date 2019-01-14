/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
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

import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.flusher.SyncFlusher;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FluencyTest
{
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTest.class);
    private Ingester ingester;
    private RecordFormatter.Instantiator recordFormatterConfig;

    @Before
    public void setUp()
    {
        ingester = mock(Ingester.class);
        recordFormatterConfig = mock(RecordFormatter.Instantiator.class);
        when(recordFormatterConfig.createInstance()).thenReturn(new JsonRecordFormatter());
    }

    @Test
    public void testIsTerminated()
            throws IOException, InterruptedException
    {
        TestableBuffer.Config bufferConfig = new TestableBuffer.Config();
        {
            Flusher.Instantiator flusherConfig = new AsyncFlusher.Config();
            Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                    recordFormatterConfig,
                    bufferConfig,
                    flusherConfig,
                    ingester);
            assertFalse(fluency.isTerminated());
            fluency.close();
            TimeUnit.SECONDS.sleep(1);
            assertTrue(fluency.isTerminated());
        }

        {
            Flusher.Instantiator flusherConfig = new SyncFlusher.Config();
            Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                    recordFormatterConfig,
                    bufferConfig,
                    flusherConfig,
                    ingester);
            assertFalse(fluency.isTerminated());
            fluency.close();
            TimeUnit.SECONDS.sleep(1);
            assertTrue(fluency.isTerminated());
        }
    }

    @Test
    public void testGetAllocatedBufferSize()
            throws IOException
    {
        Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                recordFormatterConfig,
                new TestableBuffer.Config(),
                new AsyncFlusher.Config(),
                ingester);
        assertThat(fluency.getAllocatedBufferSize(), is(0L));
        Map<String, Object> map = new HashMap<>();
        map.put("comment", "hello world");
        for (int i = 0; i < 10000; i++) {
            fluency.emit("foodb.bartbl", map);
        }
        assertThat(fluency.getAllocatedBufferSize(), is(TestableBuffer.ALLOC_SIZE * 10000L));
    }

    @Test
    public void testWaitUntilFlusherTerminated()
            throws IOException, InterruptedException
    {
        {
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config().setWaitBeforeCloseMillis(2000);
            AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config().setWaitUntilTerminated(0);
            Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                    recordFormatterConfig,
                    bufferConfig,
                    flusherConfig,
                    ingester);
            fluency.emit("foo.bar", new HashMap<>());
            fluency.close();
            assertThat(fluency.waitUntilFlusherTerminated(1), is(false));
        }

        {
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config().setWaitBeforeCloseMillis(2000);
            AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config().setWaitUntilTerminated(0);
            Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                    recordFormatterConfig,
                    bufferConfig,
                    flusherConfig,
                    ingester);
            fluency.emit("foo.bar", new HashMap<>());
            fluency.close();
            assertThat(fluency.waitUntilFlusherTerminated(3), is(true));
        }
    }

    @Test
    public void testWaitUntilFlushingAllBuffer()
            throws IOException, InterruptedException
    {
        {
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config();
            Flusher.Instantiator flusherConfig = new AsyncFlusher.Config().setFlushIntervalMillis(2000);
            Fluency fluency = null;
            try {
                fluency = new BaseFluencyBuilder().buildFromConfigs(
                        recordFormatterConfig,
                        bufferConfig,
                        flusherConfig,
                        ingester);
                fluency.emit("foo.bar", new HashMap<>());
                assertThat(fluency.waitUntilAllBufferFlushed(3), is(true));
            }
            finally {
                if (fluency != null) {
                    fluency.close();
                }
            }
        }

        {
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config();
            Flusher.Instantiator flusherConfig = new AsyncFlusher.Config().setFlushIntervalMillis(2000);
            Fluency fluency = null;
            try {
                fluency = new BaseFluencyBuilder().buildFromConfigs(
                        recordFormatterConfig,
                        bufferConfig,
                        flusherConfig,
                        ingester);
                fluency.emit("foo.bar", new HashMap<>());
                assertThat(fluency.waitUntilAllBufferFlushed(1), is(false));
            }
            finally {
                if (fluency != null) {
                    fluency.close();
                }
            }
        }
    }

    static class StuckIngester
            implements Ingester
    {
        private final CountDownLatch latch;

        StuckIngester(CountDownLatch latch)
        {
            this.latch = latch;
        }

        @Override
        public void ingest(String tag, ByteBuffer dataBuffer)
        {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                FluencyTest.LOG.warn("Interrupted in send()", e);
            }
        }

        @Override
        public Sender getSender()
        {
            return null;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

    @Test
    public void testBufferFullException()
            throws IOException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        StuckIngester stuckIngester = new StuckIngester(latch);

        try {
            Buffer.Config bufferConfig = new Buffer.Config().setChunkInitialSize(64).setMaxBufferSize(256);
            Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                    recordFormatterConfig,
                    bufferConfig,
                    new AsyncFlusher.Config(),
                    stuckIngester);
            Map<String, Object> event = new HashMap<>();
            event.put("name", "xxxx");
            for (int i = 0; i < 8; i++) {
                fluency.emit("tag", event);
            }
            try {
                fluency.emit("tag", event);
                assertTrue(false);
            }
            catch (BufferFullException e) {
                assertTrue(true);
            }
        }
        finally {
            latch.countDown();
        }
    }
}
