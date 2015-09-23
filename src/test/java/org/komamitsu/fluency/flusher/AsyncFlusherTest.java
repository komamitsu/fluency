package org.komamitsu.fluency.flusher;

import org.junit.Test;
import org.komamitsu.fluency.buffer.TestableBuffer;
import org.komamitsu.fluency.sender.MockTCPSender;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncFlusherTest
{
    @Test
    public void testAsyncFlusher()
            throws IOException, InterruptedException
    {
        TestableBuffer buffer = new TestableBuffer(new TestableBuffer.Config());
        MockTCPSender sender = new MockTCPSender(24225);
        AsyncFlusher.Config config = new AsyncFlusher.Config();
        config.setFlushIntervalMillis(500);
        Flusher flusher = config.createInstance(buffer, sender);
        assertEquals(0, buffer.getFlushCount().get());

        flusher.onUpdate();
        assertEquals(0, buffer.getFlushCount().get());

        flusher.flush();
        TimeUnit.MILLISECONDS.sleep(50);
        assertEquals(1, buffer.getFlushCount().get());

        TimeUnit.SECONDS.sleep(1);
        int count = buffer.getFlushCount().get();
        assertTrue(2 <= count && count <= 4);

        assertEquals(0, buffer.getCloseCount().get());
        flusher.close();
        assertEquals(1, buffer.getCloseCount().get());
        assertEquals(count + 1, buffer.getFlushCount().get());
    }
}