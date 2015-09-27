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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PackedForwardBuffer
    extends Buffer<PackedForwardBuffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(PackedForwardBuffer.class);
    private final Map<String, ExpirableBuffer> appendedChunks = new HashMap<String, ExpirableBuffer>();
    private final LinkedBlockingQueue<TaggableBuffer> flushableChunks = new LinkedBlockingQueue<TaggableBuffer>();
    private final AtomicInteger emitCounter = new AtomicInteger();
    private final AtomicReference<Long> lastAppendedChunksChecked = new AtomicReference<Long>();
    private final ThreadLocal<ObjectMapper> objectMapperHolder = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue()
        {
            return new ObjectMapper(new MessagePackFactory());
        }
    };
    private final ThreadLocal<ByteArrayOutputStream> outputStreamHolder = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue()
        {
            return new ByteArrayOutputStream();
        }
    };

    private PackedForwardBuffer(PackedForwardBuffer.Config bufferConfig)
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

        int delta = newChunkSize - origChunkSize;
        if (totalSize.get() + delta > bufferConfig.getBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", totalSize=" + totalSize);
        }
        totalSize.addAndGet(delta);

        ExpirableBuffer newBuffer = new ExpirableBuffer(ByteBuffer.allocate(newChunkSize));
        if (chunk != null) {
            chunk.getByteBuffer().flip();
            newBuffer.getByteBuffer().put(chunk.getByteBuffer());
        }
        LOG.trace("prepareBuffer(): allocate a new buffer. tag={}, buffer={}", tag, newBuffer);

        appendedChunks.put(tag, newBuffer);
        return newBuffer;
    }

    @Override
    public void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        ObjectMapper objectMapper = objectMapperHolder.get();
        ByteArrayOutputStream outputStream = outputStreamHolder.get();
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(timestamp, data));
        outputStream.close();

        boolean succeeded = false;
        while (!succeeded) {
            try {
                synchronized (appendedChunks) {
                    ExpirableBuffer buffer = prepareBuffer(tag, outputStream.size());
                    buffer.getByteBuffer().put(outputStream.toByteArray());
                    succeeded = true;

                    buffer.getLastUpdatedTimeMillis().set(System.currentTimeMillis());

                    moveChunkIfNeeded(tag, buffer);
                    // TODO: Configurable
                    if (emitCounter.incrementAndGet() % 1000 == 0) {
                        moveChunks(false);
                    }
                }
            }
            catch (BufferFullException e) {
                LOG.warn("Buffer is full. Maybe you'd better increase the buffer size.", e);
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                }
                catch (InterruptedException e1) {
                    LOG.warn("Interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
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
        long now = System.currentTimeMillis();
        Long lastAppendedChunksChecked = this.lastAppendedChunksChecked.get();
        if (lastAppendedChunksChecked == null) {
            this.lastAppendedChunksChecked.set(now);
        }
        else if (lastAppendedChunksChecked < now - 500) {
            moveChunks(false);
            this.lastAppendedChunksChecked.set(now);
        }

        TaggableBuffer chunk = null;
        while ((chunk = flushableChunks.poll()) != null) {
            try {
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
                synchronized (sender) {
                    sender.send(ByteBuffer.wrap(header.toByteArray()));
                    byteBuffer.flip();
                    sender.send(byteBuffer);
                }
            }
            catch (Throwable e) {
                try {
                    flushableChunks.put(chunk);
                    totalSize.addAndGet(chunk.getByteBuffer().capacity());
                }
                catch(InterruptedException e1){
                    LOG.error("Interrupted during restoring fetched chunk. It can be lost. chunk={}", chunk);
                }

                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                else {
                    throw new RuntimeException("Failed to send chunk to fluentd", e);
                }
            }
        }
    }

    @Override
    public synchronized void closeInternal(Sender sender)
            throws IOException
    {
        moveChunks(true);
        appendedChunks.clear();
        flush(sender);
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

    public static class Config extends Buffer.Config<PackedForwardBuffer, Config>
    {
        private int buffInitialSize = 512 * 1024;
        private float buffExpandRatio = 2.0f;
        private int chunkSize = 4 * 1024 * 1024;
        private int chunkRetentionTimeMillis = 500;

        public int getBuffInitialSize()
        {
            return buffInitialSize;
        }

        public Config setBuffInitialSize(int buffInitialSize)
        {
            this.buffInitialSize = buffInitialSize;
            return this;
        }

        public float getBuffExpandRatio()
        {
            return buffExpandRatio;
        }

        public Config setBuffExpandRatio(float buffExpandRatio)
        {
            this.buffExpandRatio = buffExpandRatio;
            return this;
        }

        public int getChunkSize()
        {
            return chunkSize;
        }

        public Config setChunkSize(int chunkSize)
        {
            this.chunkSize = chunkSize;
            return this;
        }

        public int getChunkRetentionTimeMillis()
        {
            return chunkRetentionTimeMillis;
        }

        public Config setChunkRetentionTimeMillis(int chunkRetentionTimeMillis)
        {
            this.chunkRetentionTimeMillis = chunkRetentionTimeMillis;
            return this;
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

        @Override
        public PackedForwardBuffer createInstance()
        {
            return new PackedForwardBuffer(this);
        }
    }
}
