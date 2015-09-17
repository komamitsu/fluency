package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Buffer
    implements Closeable
{
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

    public abstract void flush(Sender sender)
            throws IOException;

    public int getTotalSize()
    {
        return totalSize.get();
    }

    public int getMaxSize()
    {
        return bufferConfig.getBufferSize();
    }
}
