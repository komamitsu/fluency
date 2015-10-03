package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.util.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestableBuffer
        extends Buffer<TestableBuffer.Config>
{
    private final List<Tuple3<String, Long, Map<String, Object>>> events = new ArrayList<Tuple3<String, Long, Map<String, Object>>>();
    private final AtomicInteger flushCount = new AtomicInteger();
    private final AtomicInteger forceFlushCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();

    private TestableBuffer(Config bufferConfig)
    {
        super(bufferConfig);
    }

    @Override
    public void append(String tag, long timestamp, Map data)
            throws IOException
    {
        events.add(new Tuple3<String, Long, Map<String, Object>>(tag, timestamp, data));
        allocatedSize.addAndGet(100);
    }

    @Override
    public void flushInternal(Sender sender, boolean force)
            throws IOException
    {
        if (force) {
            forceFlushCount.incrementAndGet();
        }
        else {
            flushCount.incrementAndGet();
        }
        allocatedSize.set(0);
    }

    @Override
    protected void closeInternal(Sender sender)
            throws IOException
    {
        closeCount.incrementAndGet();
    }

    public List<Tuple3<String, Long, Map<String, Object>>> getEvents()
    {
        return events;
    }

    public AtomicInteger getFlushCount()
    {
        return flushCount;
    }

    public AtomicInteger getForceFlushCount()
    {
        return forceFlushCount;
    }

    public AtomicInteger getCloseCount()
    {
        return closeCount;
    }

    public static class Config
            extends Buffer.Config<TestableBuffer, Config>
    {
        @Override
        public TestableBuffer createInstance()
        {
            return new TestableBuffer(this);
        }
    }
}
