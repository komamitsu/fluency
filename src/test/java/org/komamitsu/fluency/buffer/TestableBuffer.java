package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.util.Tuple;
import org.komamitsu.fluency.util.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestableBuffer
        extends Buffer<TestableBuffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(TestableBuffer.class);
    private final List<Tuple3<String, Long, Map<String, Object>>> events = new ArrayList<Tuple3<String, Long, Map<String, Object>>>();
    private final AtomicInteger flushCount = new AtomicInteger();
    private final AtomicInteger forceFlushCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicInteger allocatedSize = new AtomicInteger();
    private final List<Tuple<List<String>, ByteBuffer>> savableBuffers = new ArrayList<Tuple<List<String>, ByteBuffer>>();
    private final List<Tuple<List<String>, ByteBuffer>> loadedBuffers = new ArrayList<Tuple<List<String>, ByteBuffer>>();

    private TestableBuffer(Config bufferConfig)
    {
        super(bufferConfig);
    }

    public void setSavableBuffer(List<String> params, ByteBuffer buffer)
    {
        savableBuffers.add(new Tuple<List<String>, ByteBuffer>(params, buffer));
    }

    @Override
    protected void loadBufferFromFile(List<String> params, FileChannel channel)
    {
        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.PRIVATE, 0, channel.size());
            loadedBuffers.add(new Tuple<List<String>, ByteBuffer>(params, buffer));
        }
        catch (IOException e) {
            LOG.warn("Failed to map the FileChannel: channel=" + channel, e);
        }
    }

    @Override
    protected void saveAllBuffersToFile()
            throws IOException
    {
        for (Tuple<List<String>, ByteBuffer> savableBuffer : savableBuffers) {
            saveBuffer(savableBuffer.getFirst(), savableBuffer.getSecond());
        }
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
    public String bufferFormatType()
    {
        return "test";
    }

    @Override
    protected void closeInternal()
    {
        closeCount.incrementAndGet();
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
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

    public List<Tuple<List<String>, ByteBuffer>> getLoadedBuffers()
    {
        return loadedBuffers;
    }

    public static class Config
            extends Buffer.Config<TestableBuffer, Config>
    {
        @Override
        protected TestableBuffer createInstanceInternal()
        {
            return new TestableBuffer(this);
        }
    }
}
