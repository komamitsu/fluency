package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class SyncFlusher
        extends Flusher
{
    private final AtomicLong lastFlushTimeMillis = new AtomicLong();

    public SyncFlusher(Buffer buffer, Sender sender, FlusherConfig flusherConfig)
    {
        super(buffer, sender, flusherConfig);
    }

    public SyncFlusher(Buffer buffer, Sender sender)
    {
        this(buffer, sender, new FlusherConfig.Builder().build());
    }

    @Override
    protected void flushInternal(boolean force)
            throws IOException
    {
        long now = System.currentTimeMillis();
        if (force ||
                now > lastFlushTimeMillis.get() + flusherConfig.getFlushIntervalMillis() ||
                buffer.getBufferUsage() > flusherConfig.getBufferOccupancyThreshold()) {
            buffer.flush(sender);
            lastFlushTimeMillis.set(now);
        }
    }

    @Override
    protected void closeInternal()
            throws IOException
    {
        flushInternal(true);
    }
}
