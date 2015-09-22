package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
    protected final Buffer buffer;
    protected final Sender sender;
    protected final Config flusherConfig;

    public Flusher(Buffer buffer, Sender sender, Config flusherConfig)
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
        closeInternal();
    }

    protected void closeBuffer()
    {
        LOG.trace("closeBuffer(): closing buffer");
        try {
            buffer.close();
        }
        catch (IOException e) {
            LOG.warn("Interrupted during closing buffer");
        }
        LOG.trace("closeBuffer(): closed buffer");
    }

    public abstract static class Config
    {
        private int flushIntervalMillis = 600;
        private float bufferOccupancyThreshold = 0.6f;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public Config setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return this;
        }

        public float getBufferOccupancyThreshold()
        {
            return bufferOccupancyThreshold;
        }

        public Config setBufferOccupancyThreshold(float bufferOccupancyThreshold)
        {
            this.bufferOccupancyThreshold = bufferOccupancyThreshold;
            return this;
        }

        public abstract Flusher createInstance(Buffer buffer, Sender sender);
    }
}
