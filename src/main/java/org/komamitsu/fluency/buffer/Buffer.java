package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Buffer
    implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    protected final BufferConfig bufferConfig;
    protected final AtomicInteger totalSize = new AtomicInteger();

    public static class BufferFullException extends IOException {
        public BufferFullException(String s)
        {
            super(s);
        }
    }

    public Buffer()
    {
        this(new BufferConfig.Builder().build());
    }

    public Buffer(BufferConfig bufferConfig)
    {
        this.bufferConfig = bufferConfig;
    }

    public abstract void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException;

    public void flush(Sender sender)
            throws IOException
    {
        flushInternal(sender);
    }

    public abstract void flushInternal(Sender sender)
            throws IOException;

    public int getTotalSize()
    {
        return totalSize.get();
    }

    public int getMaxSize()
    {
        return bufferConfig.getBufferSize();
    }

    public float getBufferUsage()
    {
        return (float)getTotalSize() / getMaxSize();
    }
}
