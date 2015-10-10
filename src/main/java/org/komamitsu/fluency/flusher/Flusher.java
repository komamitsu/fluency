package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher<C extends Flusher.Config>
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
    protected final Buffer buffer;
    protected final Sender sender;
    protected final C flusherConfig;

    public Flusher(Buffer buffer, Sender sender, C flusherConfig)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.flusherConfig = flusherConfig;
    }

    public Buffer getBuffer()
    {
        return buffer;
    }

    protected abstract void flushInternal(boolean force)
            throws IOException;

    protected abstract void closeInternal()
            throws IOException;

    public void onUpdate()
            throws IOException
    {
        flushInternal(false);
    }

    @Override
    public void flush()
            throws IOException
    {
        flushInternal(true);
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            closeInternal();
        }
        finally {
            sender.close();
        }
    }

    protected void closeBuffer()
    {
        LOG.trace("closeBuffer(): closing buffer");
        try {
            buffer.close(sender);
        }
        catch (IOException e) {
            LOG.warn("Interrupted during closing buffer");
        }
        LOG.trace("closeBuffer(): closed buffer");
    }

    public abstract static class Config<T extends Flusher, C extends Config>
    {
        private int flushIntervalMillis = 600;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public C setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return (C)this;
        }

        public abstract T createInstance(Buffer buffer, Sender sender);
    }
}
