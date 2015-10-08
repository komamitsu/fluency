package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class SyncFlusher
        extends Flusher<SyncFlusher.Config>
{
    private final AtomicLong lastFlushTimeMillis = new AtomicLong();

    private SyncFlusher(Buffer buffer, Sender sender, Config flusherConfig)
    {
        super(buffer, sender, flusherConfig);
    }

    @Override
    protected void flushInternal(boolean force)
            throws IOException
    {
        long now = System.currentTimeMillis();
        if (force ||
                now > lastFlushTimeMillis.get() + flusherConfig.getFlushIntervalMillis() ||
                buffer.getBufferUsage() > flusherConfig.getBufferOccupancyThreshold()) {
            buffer.flush(sender, force);
            lastFlushTimeMillis.set(now);
        }
    }

    @Override
    protected void closeInternal()
            throws IOException
    {
        flushInternal(true);
        closeBuffer();
    }

    public static class Config extends Flusher.Config<SyncFlusher, Config>
    {
        private float bufferOccupancyThreshold = 0.6f;

        public float getBufferOccupancyThreshold()
        {
            return bufferOccupancyThreshold;
        }

        public Config setBufferOccupancyThreshold(float bufferOccupancyThreshold)
        {
            this.bufferOccupancyThreshold = bufferOccupancyThreshold;
            return this;
        }

        @Override
        public SyncFlusher createInstance(Buffer buffer, Sender sender)
        {
            return new SyncFlusher(buffer, sender, this);
        }
    }
}
