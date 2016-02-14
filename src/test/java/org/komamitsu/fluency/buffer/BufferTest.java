package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.StubSender;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class BufferTest
{
    @Test
    public void testBuffer()
            throws IOException
    {
        TestableBuffer buffer = new TestableBuffer.Config().setMaxBufferSize(10000).createInstance();
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(1000, buffer.getAllocatedSize());
        assertEquals(0.1, buffer.getBufferUsage(), 0.001);
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(2000, buffer.getAllocatedSize());
        assertEquals(0.2, buffer.getBufferUsage(), 0.001);

        buffer.flush(new StubSender(), false);
        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferUsage(), 0.001);
    }
}