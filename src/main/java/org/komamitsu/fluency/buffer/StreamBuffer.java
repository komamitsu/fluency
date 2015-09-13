package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamBuffer
    extends Buffer
{
    private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<Event>();
    private final AtomicInteger totalSize = new AtomicInteger();

    public StreamBuffer()
    {
        this(new BufferConfig.Builder().build());
    }

    public StreamBuffer(BufferConfig bufferConfig)
    {
        super(bufferConfig);
    }

    @Override
    public synchronized void append(ByteBuffer byteBuffer)
            throws BufferFullException
    {
        if (totalSize.get() + byteBuffer.remaining() > bufferConfig.getBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", totalSize=" + totalSize + ", byteBuffer=" + byteBuffer);
        }
        events.add(new Event(byteBuffer));
        totalSize.getAndAdd(byteBuffer.remaining());
    }

    @Override
    public void flush(Sender sender)
            throws IOException
    {
        Event event = null;
        while ((event = events.poll()) != null) {
            sender.send(event.getByteBuffer());
        }
    }

    @Override
    public void close()
            throws IOException
    {
        events.clear();
    }

    public static class Event
    {
        private final ByteBuffer byteBuffer;

        public Event(ByteBuffer byteBuffer)
        {
            this.byteBuffer = byteBuffer;
        }

        public ByteBuffer getByteBuffer()
        {
            return byteBuffer;
        }
    }
}
