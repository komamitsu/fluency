package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;

public abstract class Flusher
{
    protected final Buffer buffer;
    protected final Sender sender;
    protected final FlusherConfig flusherConfig;

    public Flusher(Buffer buffer, Sender sender, FlusherConfig flusherConfig)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.flusherConfig = flusherConfig;
    }

    protected abstract void flushInternal(boolean force)
            throws IOException;

    public void onUpdate()
            throws IOException
    {
        flushInternal(false);
    }

    public void flush()
            throws IOException
    {
        flushInternal(true);
    }
}
