package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.*;
import org.msgpack.core.buffer.MessageBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PackedForwardBuffer
    extends Buffer<PackedForwardBuffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(PackedForwardBuffer.class);
    private final Map<String, RetentionBuffer> retentionBuffers = new HashMap<String, RetentionBuffer>();
    private final LinkedBlockingQueue<TaggableBuffer> flushableBuffers = new LinkedBlockingQueue<TaggableBuffer>();
    private final BufferPool bufferPool;

    private PackedForwardBuffer(PackedForwardBuffer.Config bufferConfig)
    {
        super(bufferConfig);
        bufferPool = new BufferPool(bufferConfig.getInitialBufferSize(), bufferConfig.getMaxBufferSize());
    }

    /*
    private RetentionBuffer prepareBuffer(String tag, int writeSize)
            throws BufferFullException
    {
        RetentionBuffer retentionBuffer = retentionBuffers.get(tag);
        if (retentionBuffer != null && retentionBuffer.getByteBuffer().remaining() > writeSize) {
            return retentionBuffer;
        }

        int newRetentionBufferSize;
        if (retentionBuffer == null) {
            newRetentionBufferSize = bufferConfig.getInitialBufferSize();
        }
        else{
            newRetentionBufferSize = (int) (retentionBuffer.getByteBuffer().capacity() * bufferConfig.getBufferExpandRatio());
        }

        while (newRetentionBufferSize < writeSize) {
            newRetentionBufferSize *= bufferConfig.getBufferExpandRatio();
        }

        ByteBuffer acquiredBuffer = bufferPool.acquireBuffer(newRetentionBufferSize);
        if (acquiredBuffer == null) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", bufferPool=" + bufferPool);
        }
        RetentionBuffer newBuffer = new RetentionBuffer(acquiredBuffer);
        if (retentionBuffer != null) {
            retentionBuffer.getByteBuffer().flip();
            newBuffer.getByteBuffer().put(retentionBuffer.getByteBuffer());
            bufferPool.returnBuffer(retentionBuffer.getByteBuffer());
        }
        LOG.trace("prepareBuffer(): allocate a new buffer. tag={}, buffer={}", tag, newBuffer);

        retentionBuffers.put(tag, newBuffer);
        return newBuffer;
    }
    */

    @Override
    public synchronized void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        RetentionBuffer retentionBuffer = retentionBuffers.get(tag);
        if (retentionBuffer == null) {
            retentionBuffer = new RetentionBuffer();
            retentionBuffers.put(tag, retentionBuffer);
        }
        MessagePacker messagePacker = new MessagePacker(retentionBuffer.bufferOutput);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            messagePacker.packString(entry.getKey());
            Object value = entry.getValue();
            if (value instanceof String) {
                messagePacker.packString((String) value);
            }
            else if (value instanceof Integer) {
                messagePacker.packInt((Integer) value);
            }
            else if (value instanceof Long) {
                messagePacker.packLong((Long) value);
            }
            else if (value instanceof Float) {
                messagePacker.packFloat((Float) value);
            }
            else if (value instanceof Double) {
                messagePacker.packDouble((Double) value);
            }
            else {
                throw new RuntimeException("Not supported yet: " + value.getClass());
            }
        }
        retentionBuffer.getLastUpdatedTimeMillis().set(System.currentTimeMillis());
        moveRetentionBufferIfNeeded(tag, retentionBuffer);

        /*
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
        */
    }

    private void moveRetentionBufferIfNeeded(String tag, RetentionBuffer buffer)
            throws IOException
    {
        if (buffer.getBufferOutput().getTotalWrittenBytes() > bufferConfig.getBufferRetentionSize()) {
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
            flushableBuffers.put(new TaggableBuffer(tag, buffer.getBufferOutput().getTotalWrittenBytes(), buffer.getBufferOutput().getMessageBuffers()));
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
            try {
                // TODO: Reuse MessagePacker
                ByteArrayOutputStream header = new ByteArrayOutputStream();
                MessagePacker messagePacker = MessagePack.newDefaultPacker(header);
                LOG.trace("flushInternal(): bufferUsage={}, flushableBuffer={}", getBufferUsage(), flushableBuffer);
                String tag = flushableBuffer.getTag();
                if (bufferConfig.isAckResponseMode()) {
                    messagePacker.packArrayHeader(3);
                }
                else {
                    messagePacker.packArrayHeader(2);
                }
                messagePacker.packString(tag);
                messagePacker.packRawStringHeader(flushableBuffer.getBufferLength());
                messagePacker.flush();

                synchronized (sender) {
                    ByteBuffer headerBuffer = ByteBuffer.wrap(header.toByteArray());
                    List<ByteBuffer> byteBuffers = new LinkedList<ByteBuffer>();
                    byteBuffers.add(headerBuffer);
                    for (MessageBuffer messageBuffer : flushableBuffer.getMessageBuffers()) {
                        byteBuffers.add(messageBuffer.getReference());
                    }

                    if (bufferConfig.isAckResponseMode()) {
                        String uuid = UUID.randomUUID().toString();
                        sender.sendWithAck(byteBuffers, uuid.getBytes(CHARSET));
                    }
                    else {
                        sender.send(byteBuffers);
                    }
                }
            }
            finally {
//                bufferPool.returnBuffer(flushableBuffer.getByteBuffer());
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
        bufferPool.releaseBuffers();
    }

    @Override
    public long getAllocatedSize()
    {
        return bufferPool.getAllocatedSize();
    }

    private static class RetentionBuffer
    {
        private final AtomicLong lastUpdatedTimeMillis = new AtomicLong();
        private final DirectByteBufferOutput bufferOutput = new DirectByteBufferOutput();

        public AtomicLong getLastUpdatedTimeMillis()
        {
            return lastUpdatedTimeMillis;
        }

        public DirectByteBufferOutput getBufferOutput()
        {
            return bufferOutput;
        }

        @Override
        public String toString()
        {
            return "RetentionBuffer{" +
                    "lastUpdatedTimeMillis=" + lastUpdatedTimeMillis +
                    ", bufferOutput=" + bufferOutput +
                    '}';
        }
    }

    private static class TaggableBuffer
    {
        private final String tag;
        private final int bufferLength;
        private final List<MessageBuffer> messageBuffers;

        public TaggableBuffer(String tag, int bufferLength, List<MessageBuffer> messageBuffers)
        {
            this.tag = tag;
            this.bufferLength = bufferLength;
            this.messageBuffers = messageBuffers;
        }

        public String getTag()
        {
            return tag;
        }

        public int getBufferLength()
        {
            return bufferLength;
        }

        public List<MessageBuffer> getMessageBuffers()
        {
            return messageBuffers;
        }

        @Override
        public String toString()
        {
            return "TaggableBuffer{" +
                    "tag='" + tag + '\'' +
                    ", bufferLength=" + bufferLength +
                    ", messageBuffers=" + messageBuffers +
                    '}';
        }
    }

    public static class Config extends Buffer.Config<PackedForwardBuffer, Config>
    {
        private int initialBufferSize = 1024 * 1024;
        private float bufferExpandRatio = 2.0f;
        private int bufferRetentionSize = 4 * 1024 * 1024;
        private int bufferRetentionTimeMillis = 400;

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
