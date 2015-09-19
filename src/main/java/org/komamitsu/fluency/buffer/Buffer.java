package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Buffer<T extends Buffer.Config>
    implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    protected final T bufferConfig;
    protected final AtomicInteger totalSize = new AtomicInteger();

    public static class BufferFullException extends IOException {
        public BufferFullException(String s)
        {
            super(s);
        }
    }

    public Buffer(T bufferConfig)
    {
        this.bufferConfig = bufferConfig;
    }

    public abstract void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException;

    public void flush(Sender sender)
            throws IOException
    {
        LOG.trace("flush(): bufferUsage={}", getBufferUsage());
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

    public abstract static class Config
    {
        private int bufferSize = 16 * 1024 * 1024;
        private int chunkSize = 512 * 1024;

        public int getBufferSize()
        {
            return bufferSize;
        }

        public void setBufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
        }

        public int getChunkSize()
        {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize)
        {
            this.chunkSize = chunkSize;
        }
    }
}
