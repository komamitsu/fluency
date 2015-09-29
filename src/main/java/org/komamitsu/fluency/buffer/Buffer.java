package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Buffer<T extends Buffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    protected final T bufferConfig;
    protected final AtomicInteger allocatedSize = new AtomicInteger();

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

    public void close(Sender sender)
            throws IOException
    {
        closeInternal(sender);
    }

    protected abstract void closeInternal(Sender sender)
            throws IOException;

    public int getAllocatedSize()
    {
        return allocatedSize.get();
    }

    public int getMaxSize()
    {
        return bufferConfig.getMaxBufferSize();
    }

    public float getBufferUsage()
    {
        return (float) getAllocatedSize() / getMaxSize();
    }

    public abstract static class Config<T extends Buffer, C extends Config>
    {
        protected int maxBufferSize = 16 * 1024 * 1024;

        public int getMaxBufferSize()
        {
            return maxBufferSize;
        }

        public C setMaxBufferSize(int maxBufferSize)
        {
            this.maxBufferSize = maxBufferSize;
            return (C)this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "maxBufferSize=" + maxBufferSize +
                    '}';
        }

        public abstract T createInstance();
    }
}
