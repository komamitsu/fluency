package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
        public void close()
                throws IOException
        {
        }
    }

    private static class TestableBuffer
            extends Buffer<TestableBuffer.Config>
    {
        public TestableBuffer(Config bufferConfig)
        {
            super(bufferConfig);
        }

        @Override
        public void append(String tag, long timestamp, Map data)
                throws IOException
        {
            totalSize.addAndGet(100);
        }

        @Override
        public void flushInternal(Sender sender)
                throws IOException
        {
            totalSize.set(0);
        }

        @Override
        protected void closeInternal(Sender sender)
                throws IOException
        {
        }

        static class Config
                extends Buffer.Config<Config>
        {
        }
    }

    @Test
    public void testBuffer()
            throws IOException
    {
        TestableBuffer buffer = new TestableBuffer(new TestableBuffer.Config().setBufferSize(10000));
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(1000, buffer.getTotalSize());
        assertEquals(0.1, buffer.getBufferUsage(), 0.001);
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(2000, buffer.getTotalSize());
        assertEquals(0.2, buffer.getBufferUsage(), 0.001);

        buffer.flush(new StabSender());
        assertEquals(0, buffer.getTotalSize());
        assertEquals(0, buffer.getBufferUsage(), 0.001);
    }
}