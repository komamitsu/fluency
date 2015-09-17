package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;

import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher
{
    protected final FlusherConfig flusherConfig;

    public Flusher(FlusherConfig flusherConfig)
    {
        this.flusherConfig = flusherConfig;
    }

    protected abstract void flushInternal(Flushable flushable, Buffer buffer, boolean force)
            throws IOException;

    public void onUpdate(Flushable flushable, Buffer buffer)
            throws IOException
    {
        flushInternal(flushable, buffer, false);
    }

    public void flush(Flushable flushable, Buffer buffer)
            throws IOException
    {
        flushInternal(flushable, buffer, true);
    }
}
