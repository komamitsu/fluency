package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher implements Flushable, Closeable
{
    protected final Buffer buffer;
    protected final Sender sender;
    protected final Config flusherConfig;

    public Flusher(Buffer buffer, Sender sender, Config flusherConfig)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.flusherConfig = flusherConfig;
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
        buffer.close();
        closeInternal();
    }

    public static class Config
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
    }
}
