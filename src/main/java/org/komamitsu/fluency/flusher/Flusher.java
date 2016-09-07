package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
    protected final Buffer buffer;
    protected final Sender sender;
    private final Config config;

    public Flusher(Buffer buffer, Sender sender, Config config)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.config = config;
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

    public abstract boolean isTerminated();

    protected void closeBuffer()
    {
        LOG.trace("closeBuffer(): closing buffer");
        buffer.close();
    }

    public static class Config
    {
        private int flushIntervalMillis = 600;
        private int waitAfterClose = 10;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public Config setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return this;
        }

        public int getWaitAfterClose()
        {
            return waitAfterClose;
        }

        public Config setWaitAfterClose(int waitAfterClose)
        {
            this.waitAfterClose = waitAfterClose;
            return this;
        }
    }

    public interface Instantiator
    {
        Flusher createInstance(Buffer buffer, Sender sender);
    }
}
