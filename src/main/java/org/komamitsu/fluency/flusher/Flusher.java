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
    protected final Flusher.Config flusherConfig;

    public Flusher(Buffer buffer, Sender sender, Flusher.Config flusherConfig)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.flusherConfig = flusherConfig;
    }

    protected Flusher.Config getConfig() {
        return flusherConfig;
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

    public abstract static class Config<FlusherImpl extends Flusher, ConfigImpl extends Config>
    {
        private int flushIntervalMillis = 600;

        private int waitAfterClose = 10;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public ConfigImpl setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return self();
        }

        public int getWaitAfterClose()
        {
            return waitAfterClose;
        }

        public ConfigImpl setWaitAfterClose(int waitAfterClose)
        {
            this.waitAfterClose = waitAfterClose;
            return self();
        }

        protected abstract ConfigImpl self();
        public abstract FlusherImpl createInstance(Buffer buffer, Sender sender);
    }
}
