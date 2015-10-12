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
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PackedForwardBuffer
    extends Buffer<PackedForwardBuffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(PackedForwardBuffer.class);
    private final Map<String, RetentionBuffer> retentionBuffers = new HashMap<String, RetentionBuffer>();
    private final LinkedBlockingQueue<TaggableBuffer> flushableBuffers = new LinkedBlockingQueue<TaggableBuffer>();

    private PackedForwardBuffer(PackedForwardBuffer.Config bufferConfig)
    {
        super(bufferConfig);
    }

    private synchronized RetentionBuffer prepareBuffer(String tag, int writeSize)
            throws BufferFullException
    {
        RetentionBuffer retentionBuffer = retentionBuffers.get(tag);
        if (retentionBuffer != null && retentionBuffer.getByteBuffer().remaining() > writeSize) {
            return retentionBuffer;
        }

        int origRetentionBufferSize;
        int newRetentionBufferSize;
        if (retentionBuffer == null) {
            origRetentionBufferSize = 0;
            newRetentionBufferSize = bufferConfig.getInitialBufferSize();
        }
        else{
            origRetentionBufferSize = retentionBuffer.getByteBuffer().capacity();
            newRetentionBufferSize = (int) (retentionBuffer.getByteBuffer().capacity() * bufferConfig.getBufferExpandRatio());
        }

        while (newRetentionBufferSize < writeSize) {
            newRetentionBufferSize *= bufferConfig.getBufferExpandRatio();
        }

        int delta = newRetentionBufferSize - origRetentionBufferSize;
        if (allocatedSize.get() + delta > bufferConfig.getMaxBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", allocatedSize=" + allocatedSize);
        }
        allocatedSize.addAndGet(delta);

        RetentionBuffer newBuffer = new RetentionBuffer(ByteBuffer.allocate(newRetentionBufferSize));
        if (retentionBuffer != null) {
            retentionBuffer.getByteBuffer().flip();
            newBuffer.getByteBuffer().put(retentionBuffer.getByteBuffer());
        }
        LOG.trace("prepareBuffer(): allocate a new buffer. tag={}, buffer={}", tag, newBuffer);

        retentionBuffers.put(tag, newBuffer);
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

        synchronized (retentionBuffers) {
            RetentionBuffer buffer = prepareBuffer(tag, outputStream.size());
            buffer.getByteBuffer().put(outputStream.toByteArray());
            buffer.getLastUpdatedTimeMillis().set(System.currentTimeMillis());
            moveRetentionBufferIfNeeded(tag, buffer);
        }
    }

    private void moveRetentionBufferIfNeeded(String tag, RetentionBuffer buffer)
            throws IOException
    {
        if (buffer.getByteBuffer().position() > bufferConfig.getBufferRetentionSize()) {
            moveRetentionBufferToFlushable(tag, buffer);
        }
    }

    private void moveRetentionBuffersToFlushable(boolean force)
            throws IOException
    {
        long expiredThreshold = System.currentTimeMillis() - bufferConfig.getBufferRetentionTimeMillis();

        synchronized (retentionBuffers) {
            for (Map.Entry<String, RetentionBuffer> entry : retentionBuffers.entrySet()) {
                // it can be null because moveRetentionBufferToFlushable() can set null
                if (entry.getValue() != null) {
                    if (force || entry.getValue().getLastUpdatedTimeMillis().get() < expiredThreshold) {
                        moveRetentionBufferToFlushable(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
    }

    private void moveRetentionBufferToFlushable(String tag, RetentionBuffer buffer)
            throws IOException
    {
        try {
            LOG.trace("moveRetentionBufferToFlushable(): tag={}, buffer={}", tag, buffer);
            flushableBuffers.put(new TaggableBuffer(tag, buffer.getByteBuffer()));
            retentionBuffers.put(tag, null);
        }
        catch (InterruptedException e) {
            throw new IOException("Failed to move retention buffer due to interruption", e);
        }
    }

    @Override
    public void flushInternal(Sender sender, boolean force)
            throws IOException
    {
        moveRetentionBuffersToFlushable(force);

        TaggableBuffer flushableBuffer = null;
        while ((flushableBuffer = flushableBuffers.poll()) != null) {
            allocatedSize.addAndGet(-flushableBuffer.getByteBuffer().capacity());
            // TODO: Reuse MessagePacker
            ByteArrayOutputStream header = new ByteArrayOutputStream();
            MessagePacker messagePacker = MessagePack.newDefaultPacker(header);
            LOG.trace("flushInternal(): bufferUsage={}, flushableBuffer={}", getBufferUsage(), flushableBuffer);
            String tag = flushableBuffer.getTag();
            ByteBuffer byteBuffer = flushableBuffer.getByteBuffer();
            if (bufferConfig.isAckResponseMode()) {
                messagePacker.packArrayHeader(3);
            }
            else {
                messagePacker.packArrayHeader(2);
            }
            messagePacker.packString(tag);
            messagePacker.packRawStringHeader(byteBuffer.position());
            messagePacker.flush();

            synchronized (sender) {
                ByteBuffer headerBuffer = ByteBuffer.wrap(header.toByteArray());
                byteBuffer.flip();
                if (bufferConfig.isAckResponseMode()) {
                    String uuid = UUID.randomUUID().toString();
                    sender.sendWithAck(Arrays.asList(headerBuffer, byteBuffer), uuid.getBytes(CHARSET));
                }
                else {
                    sender.send(Arrays.asList(headerBuffer, byteBuffer));
                }
            }
        }
    }

    @Override
    public synchronized void closeInternal(Sender sender)
            throws IOException
    {
        moveRetentionBuffersToFlushable(true);
        retentionBuffers.clear();
        flush(sender, true);
    }

    private static class RetentionBuffer
    {
        private final AtomicLong lastUpdatedTimeMillis = new AtomicLong();
        private final ByteBuffer byteBuffer;

        public RetentionBuffer(ByteBuffer byteBuffer)
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
        private int initialBufferSize = 512 * 1024;
        private float bufferExpandRatio = 2.0f;
        private int bufferRetentionSize = 4 * 1024 * 1024;
        private int bufferRetentionTimeMillis = 500;

        public int getInitialBufferSize()
        {
            return initialBufferSize;
        }

        public Config setInitialBufferSize(int initialBufferSize)
        {
            this.initialBufferSize = initialBufferSize;
            return this;
        }

        public float getBufferExpandRatio()
        {
            return bufferExpandRatio;
        }

        public Config setBufferExpandRatio(float bufferExpandRatio)
        {
            this.bufferExpandRatio = bufferExpandRatio;
            return this;
        }

        public int getBufferRetentionSize()
        {
            return bufferRetentionSize;
        }

        public Config setBufferRetentionSize(int bufferRetentionSize)
        {
            this.bufferRetentionSize = bufferRetentionSize;
            return this;
        }

        public int getBufferRetentionTimeMillis()
        {
            return bufferRetentionTimeMillis;
        }

        public Config setBufferRetentionTimeMillis(int bufferRetentionTimeMillis)
        {
            this.bufferRetentionTimeMillis = bufferRetentionTimeMillis;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "initialBufferSize=" + initialBufferSize +
                    ", bufferExpandRatio=" + bufferExpandRatio +
                    ", bufferRetentionSize=" + bufferRetentionSize +
                    ", bufferRetentionTimeMillis=" + bufferRetentionTimeMillis +
                    "} " + super.toString();
        }

        @Override
        public PackedForwardBuffer createInstance()
        {
            return new PackedForwardBuffer(this);
        }
    }
}
