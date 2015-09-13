package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

abstract class Buffer
    implements Closeable
{
    protected final BufferConfig bufferConfig;

    public static class BufferFullException extends IOException {
        public BufferFullException(String s)
        {
            super(s);
        }
    }

    public Buffer()
    {
        this(new BufferConfig.Builder().build());
    }

    public Buffer(BufferConfig bufferConfig)
    {
        this.bufferConfig = bufferConfig;
    }

    public abstract void append(ByteBuffer byteBuffer)
            throws BufferFullException;

    public abstract void flush(Sender sender)
            throws IOException;
}
