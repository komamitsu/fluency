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

    public void close(Sender sender)
            throws IOException
    {
        closeInternal(sender);
    }

    protected abstract void closeInternal(Sender sender)
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

    public abstract static class Config<T extends Buffer, C extends Config>
    {
        protected int bufferSize = 16 * 1024 * 1024;

        public int getBufferSize()
        {
            return bufferSize;
        }

        public C setBufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
            return (C)this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "bufferSize=" + bufferSize +
                    '}';
        }

        public abstract T createInstance();
    }
}
