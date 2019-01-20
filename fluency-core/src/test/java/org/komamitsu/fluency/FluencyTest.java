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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.Sender;
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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class FluencyTest
{
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTest.class);
    private Ingester ingester;
    private Buffer.Config bufferConfig;
    private AsyncFlusher.Config flusherConfig;

    @BeforeEach
    void setUp()
    {
        ingester = mock(Ingester.class);

        bufferConfig = new Buffer.Config();
        flusherConfig = new AsyncFlusher.Config();
    }

    @Test
    void testIsTerminated()
            throws IOException, InterruptedException
    {
        Buffer buffer = new Buffer(bufferConfig, new JsonRecordFormatter());
        Flusher flusher = new AsyncFlusher(flusherConfig, buffer, ingester);
        try (Fluency fluency = new Fluency(buffer, flusher)) {
            assertFalse(fluency.isTerminated());
            fluency.close();
            TimeUnit.SECONDS.sleep(1);
            assertTrue(fluency.isTerminated());
        }
    }

    @Test
    void testGetAllocatedBufferSize()
            throws IOException
    {
        Buffer buffer = new TestableBuffer(new TestableBuffer.Config());
        Flusher flusher = new AsyncFlusher(flusherConfig, buffer, ingester);
        try (Fluency fluency = new Fluency(buffer, flusher)) {
            assertThat(fluency.getAllocatedBufferSize(), is(0L));
            Map<String, Object> map = new HashMap<>();
            map.put("comment", "hello world");
            for (int i = 0; i < 10000; i++) {
                fluency.emit("foodb.bartbl", map);
            }
            assertThat(fluency.getAllocatedBufferSize(), is(TestableBuffer.ALLOC_SIZE * 10000L));
        }
    }

    @ParameterizedTest
    @CsvSource({"1, false","3, true"})
    void testWaitUntilFlusherTerminated(int waitUntilFlusherTerm, boolean expected)
            throws IOException, InterruptedException
    {
        flusherConfig.setWaitUntilTerminated(0);

        // Wait before actually closing in Buffer
        int waitBeforeCloseMillis = 2000;
        Buffer buffer = spy(new Buffer(bufferConfig, new JsonRecordFormatter()));
        doAnswer((invocation) -> {
            long start = System.currentTimeMillis();
            try {
                TimeUnit.MILLISECONDS.sleep(waitBeforeCloseMillis);
            }
            catch (InterruptedException e) {
                long rest = waitBeforeCloseMillis - (System.currentTimeMillis() - start);
                if (rest > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(rest);
                    }
                    catch (InterruptedException e1) {
                    }
                }
            }
            return null;
        }).doCallRealMethod().when(buffer).close();

        Flusher flusher = new AsyncFlusher(flusherConfig, buffer, ingester);
        Fluency fluency = new Fluency(buffer, flusher);

        fluency.emit("foo.bar", new HashMap<>());
        fluency.close();
        assertThat(fluency.waitUntilFlusherTerminated(waitUntilFlusherTerm), is(expected));
    }

    @ParameterizedTest
    @CsvSource({"1, false", "3, true"})
    void testWaitUntilFlushingAllBuffer(int waitUntilFlusherTerm, boolean expected)
            throws IOException, InterruptedException
    {
        flusherConfig.setFlushIntervalMillis(2000);

        Buffer buffer = new Buffer(bufferConfig, new JsonRecordFormatter());
        Flusher flusher = new AsyncFlusher(flusherConfig, buffer, ingester);
        try (Fluency fluency = new Fluency(buffer, flusher)) {
            fluency.emit("foo.bar", new HashMap<>());
            assertThat(fluency.waitUntilAllBufferFlushed(waitUntilFlusherTerm), is(expected));
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

        bufferConfig.setChunkInitialSize(64);
        bufferConfig.setChunkExpandRatio(2);
        bufferConfig.setMaxBufferSize(256);
        flusherConfig.setFlushIntervalMillis(1000);

        Buffer buffer = new Buffer(bufferConfig, new JsonRecordFormatter());
        Flusher flusher = new AsyncFlusher(flusherConfig, buffer, stuckIngester);
        try (Fluency fluency = new Fluency(buffer, flusher)) {
            Map<String, Object> event = new HashMap<>();
            event.put("name", "xxxx");  // '{"name":"xxxx"}' (length: 15 bytes)
            // Buffers: 64 + 128 = 192
            //          64 + 128 + 256 = 448 > 256
            // 15 * (8 + 1) = 135
            for (int i = 0; i < 8; i++) {
                fluency.emit("tag", event);
            }
            try {
                fluency.emit("tag", event);
                fail();
            }
            catch (BufferFullException e) {
                assertTrue(true);
            }
            finally {
                latch.countDown();
            }
        }
    }
}
