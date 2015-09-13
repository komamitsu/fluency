package org.komamitsu.fluency.flusher;

import java.io.Flushable;
import java.io.IOException;

public abstract class Flusher
{
    protected final FlusherConfig flusherConfig;

    public Flusher(FlusherConfig flusherConfig)
    {
        this.flusherConfig = flusherConfig;
    }

    protected abstract void flushInternal(Flushable flushable, boolean force)
            throws IOException;

    public void onUpdate(Flushable flushable)
            throws IOException
    {
        flushInternal(flushable, false);
    }

    public void flush(Flushable flushable)
            throws IOException
    {
        flushInternal(flushable, true);
    }
}
