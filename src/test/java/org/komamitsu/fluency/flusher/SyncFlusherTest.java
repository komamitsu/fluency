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