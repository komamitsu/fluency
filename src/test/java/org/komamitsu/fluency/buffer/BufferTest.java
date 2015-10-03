package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class BufferTest
{
    private static class StabSender
            implements Sender
    {
        @Override
        public void send(ByteBuffer data)
                throws IOException
        {
        }

        @Override
        public void send(List<ByteBuffer> dataList)
                throws IOException
        {
        }

        @Override
        public void sendWithAck(List<ByteBuffer> dataList, String uuid)
                throws IOException
        {
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

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

        buffer.flush(new StabSender(), false);
        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferUsage(), 0.001);
    }
}