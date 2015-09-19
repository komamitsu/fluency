package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PackedForwardBuffer
    extends Buffer<PackedForwardBuffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(PackedForwardBuffer.class);
    private final Map<String, ExpirableBuffer> appendedChunks = new HashMap<String, ExpirableBuffer>();
    private final LinkedBlockingQueue<TaggableBuffer> flushableChunks = new LinkedBlockingQueue<TaggableBuffer>();
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final AtomicInteger emitCounter = new AtomicInteger();

    public PackedForwardBuffer()
    {
        this(new Config());
    }

    public PackedForwardBuffer(PackedForwardBuffer.Config bufferConfig)
    {
        super(bufferConfig);
    }

    private synchronized ExpirableBuffer prepareBuffer(String tag, int writeSize)
            throws BufferFullException
    {
        ExpirableBuffer chunk = appendedChunks.get(tag);
        if (chunk != null && chunk.getByteBuffer().remaining() > writeSize) {
            return chunk;
        }

        int origChunkSize;
        int newChunkSize;
        if (chunk == null) {
            origChunkSize = 0;
            newChunkSize = bufferConfig.getBuffInitialSize();
        }
        else{
            origChunkSize = chunk.getByteBuffer().capacity();
            newChunkSize = (int) (chunk.getByteBuffer().capacity() * bufferConfig.getBuffExpandRatio());
        }

        while (newChunkSize < writeSize) {
            newChunkSize *= bufferConfig.getBuffExpandRatio();
        }

        totalSize.addAndGet(newChunkSize - origChunkSize);
        if (totalSize.get() > bufferConfig.getBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", totalSize=" + totalSize);
        }

        ExpirableBuffer newBuffer = new ExpirableBuffer(ByteBuffer.allocate(newChunkSize));
        if (chunk != null) {
            chunk.getByteBuffer().flip();
            newBuffer.getByteBuffer().put(chunk.getByteBuffer());
        }
        LOG.trace("prepareBuffer(): allocate a new buffer. buffer={}", newBuffer);

        appendedChunks.put(tag, newBuffer);
        return newBuffer;
    }

    @Override
    public synchronized void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(timestamp, data));
        outputStream.close();

        ExpirableBuffer buffer = prepareBuffer(tag, outputStream.size());
        buffer.getByteBuffer().put(outputStream.toByteArray());

        buffer.getLastUpdatedTimeMillis().set(System.currentTimeMillis());

        moveChunkIfNeeded(tag, buffer);
        // TODO: Configurable
        if (emitCounter.incrementAndGet() % 1000 == 0) {
            moveChunks(false);
        }
    }

    private synchronized void moveChunkIfNeeded(String tag, ExpirableBuffer buffer)
            throws IOException
    {
        if (buffer.getByteBuffer().position() > bufferConfig.getChunkSize()) {
            moveChunk(tag, buffer);
        }
    }

    private synchronized void moveChunks(boolean force)
            throws IOException
    {
        long expiredThreshold = System.currentTimeMillis() - bufferConfig.getChunkRetentionTimeMillis();
        for (Map.Entry<String, ExpirableBuffer> entry : appendedChunks.entrySet()) {
            // it can be null because moveChunk() can set null
            if (entry.getValue() != null) {
                if (force || entry.getValue().getLastUpdatedTimeMillis().get() < expiredThreshold) {
                    moveChunk(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private synchronized void moveChunk(String tag, ExpirableBuffer buffer)
            throws IOException
    {
        try {
            LOG.trace("moveChunk(): tag={}, buffer={}", tag, buffer);
            flushableChunks.put(new TaggableBuffer(tag, buffer.getByteBuffer()));
            appendedChunks.put(tag, null);
        }
        catch (InterruptedException e) {
            throw new IOException("Failed to move chunk due to interruption", e);
        }
    }

    @Override
    public void flushInternal(Sender sender)
            throws IOException
    {
        // TODO: Consider the memory size of `flushableChunks` as well as `appendedChunks`
        // TODO: Consider the flow control during appending events
        TaggableBuffer chunk = null;
        while ((chunk = flushableChunks.poll()) != null) {
            totalSize.addAndGet(-chunk.getByteBuffer().capacity());
            // TODO: Reuse MessagePacker
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            MessagePacker messagePacker = MessagePack.newDefaultPacker(header);
            LOG.trace("flushInternal(): bufferUsage={}, chunk={}", getBufferUsage(), chunk);
            String tag = chunk.getTag();
            ByteBuffer byteBuffer = chunk.getByteBuffer();
            messagePacker.packArrayHeader(2);
            messagePacker.packString(tag);
            messagePacker.packRawStringHeader(byteBuffer.position());
            messagePacker.flush();
            sender.send(ByteBuffer.wrap(header.toByteArray()));
            byteBuffer.flip();
            sender.send(byteBuffer);
        }
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        moveChunks(true);
        appendedChunks.clear();
    }

    private static class ExpirableBuffer
    {
        private final AtomicLong lastUpdatedTimeMillis = new AtomicLong();
        private final ByteBuffer byteBuffer;

        public ExpirableBuffer(ByteBuffer byteBuffer)
        {
            this.byteBuffer = byteBuffer;
        }

        public AtomicLong getLastUpdatedTimeMillis()
        {
            return lastUpdatedTimeMillis;
        }

        public ByteBuffer getByteBuffer()
        {
            return byteBuffer;
        }

        @Override
        public String toString()
        {
            return "ExpirableBuffer{" +
                    "lastUpdatedTimeMillis=" + lastUpdatedTimeMillis +
                    ", byteBuffer=" + byteBuffer +
                    '}';
        }
    }

    private static class TaggableBuffer
    {
        private final String tag;
        private final ByteBuffer byteBuffer;

        public TaggableBuffer(String tag, ByteBuffer byteBuffer)
        {
            this.tag = tag;
            this.byteBuffer = byteBuffer;
        }

        public String getTag()
        {
            return tag;
        }

        public ByteBuffer getByteBuffer()
        {
            return byteBuffer;
        }

        @Override
        public String toString()
        {
            return "TaggableBuffer{" +
                    "tag='" + tag + '\'' +
                    ", byteBuffer=" + byteBuffer +
                    '}';
        }
    }

    public static class Config extends Buffer.Config<Config>
    {
        private int buffInitialSize = 512 * 1024;
        private float buffExpandRatio = 1.5f;
        private int chunkSize = 1024 * 1024;
        private int chunkRetentionTimeMillis = 2 * 1000;

        public int getBuffInitialSize()
        {
            return buffInitialSize;
        }

        public void setBuffInitialSize(int buffInitialSize)
        {
            this.buffInitialSize = buffInitialSize;
        }

        public float getBuffExpandRatio()
        {
            return buffExpandRatio;
        }

        public void setBuffExpandRatio(float buffExpandRatio)
        {
            this.buffExpandRatio = buffExpandRatio;
        }

        public int getChunkSize()
        {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize)
        {
            this.chunkSize = chunkSize;
        }

        public int getChunkRetentionTimeMillis()
        {
            return chunkRetentionTimeMillis;
        }

        public void setChunkRetentionTimeMillis(int chunkRetentionTimeMillis)
        {
            this.chunkRetentionTimeMillis = chunkRetentionTimeMillis;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "buffInitialSize=" + buffInitialSize +
                    ", buffExpandRatio=" + buffExpandRatio +
                    ", chunkSize=" + chunkSize +
                    ", chunkRetentionTimeMillis=" + chunkRetentionTimeMillis +
                    "} " + super.toString();
        }
    }
}
