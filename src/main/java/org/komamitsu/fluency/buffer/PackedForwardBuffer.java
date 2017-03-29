package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.Module;
import org.komamitsu.fluency.BufferFullException;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class PackedForwardBuffer
    extends Buffer
{
    public static final String FORMAT_TYPE = "packed_forward";
    private static final Logger LOG = LoggerFactory.getLogger(PackedForwardBuffer.class);
    private final RetentionBuffers retentionBuffers = new RetentionBuffers();
    private final LinkedBlockingQueue<TaggableBuffer> flushableBuffers = new LinkedBlockingQueue<TaggableBuffer>();
    private final Queue<TaggableBuffer> backupBuffers = new ConcurrentLinkedQueue<TaggableBuffer>();
    private final BufferPool bufferPool;
    private final Config config;

    private static class RetentionBuffers {
        private static final int STRIPE_NUM = 32;
        private final Map<String, RetentionBuffer> buffers = new ConcurrentHashMap<String, RetentionBuffer>();
        private final ReentrantLock[] locks = new ReentrantLock[STRIPE_NUM];

        RetentionBuffers()
        {
            for (int i = 0; i < STRIPE_NUM; i++) {
                locks[i] = new ReentrantLock();
            }
        }

        RetentionBuffer get(String tag)
        {
            return buffers.get(tag);
        }

        void put(String tag, RetentionBuffer buffer)
        {
            buffers.put(tag, buffer);
        }

        void remove(String tag)
        {
            buffers.remove(tag);
        }

        void foreach(IterateAction iterateAction, boolean withLock)
        {
            for (Map.Entry<String, RetentionBuffer> entry : buffers.entrySet()) {
                String tag = entry.getKey();
                if (withLock) {
                    lock(tag);
                }
                try {
                    iterateAction.iterate(tag, entry.getValue());
                }
                finally {
                    if (withLock) {
                        unlock(tag);
                    }
                }
            }
        }

        <E extends Exception> void foreachWithException(ThrowableIterateAction<E> iterateAction, boolean withLock)
                throws E
        {
            for (Map.Entry<String, RetentionBuffer> entry : buffers.entrySet()) {
                String tag = entry.getKey();
                if (withLock) {
                    lock(tag);
                }
                try {
                    iterateAction.<E>iterateWithException(entry.getKey(), entry.getValue());
                }
                finally {
                    if (withLock) {
                        unlock(tag);
                    }
                }
            }
        }

        void clear()
        {
            buffers.clear();
        }

        private ReentrantLock getLock(String tag)
        {
            return locks[Math.abs(tag.hashCode() % STRIPE_NUM)];
        }

        void lock(String tag)
        {
            ReentrantLock lock = getLock(tag);
            lock.lock();
        }

        void unlock(String tag)
        {
            ReentrantLock lock = getLock(tag);
            lock.unlock();
        }

        interface IterateAction {
            void iterate(String tag, RetentionBuffer buffer);
        }

        interface ThrowableIterateAction <E extends Exception> {
            void iterateWithException(String tag, RetentionBuffer buffer)
                    throws E;
        }
    }

    protected PackedForwardBuffer(PackedForwardBuffer.Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
        if (config.getChunkInitialSize() > config.getChunkRetentionSize()) {
            LOG.warn("Initial Buffer Chunk Size ({}) shouldn't be more than Buffer Chunk Retention Size ({}) for better performance.",
                    config.getChunkInitialSize(), config.getChunkRetentionSize());
        }
        bufferPool = new BufferPool(config.getChunkInitialSize(), config.getMaxBufferSize());
    }

    private RetentionBuffer prepareBuffer(String tag, int writeSize)
            throws BufferFullException
    {
        RetentionBuffer retentionBuffer = retentionBuffers.get(tag);
        if (retentionBuffer != null && retentionBuffer.getByteBuffer().remaining() > writeSize) {
            return retentionBuffer;
        }

        int existingDataSize = 0;
        int newBufferChunkRetentionSize;
        if (retentionBuffer == null) {
            newBufferChunkRetentionSize = config.getChunkInitialSize();
        }
        else{
            existingDataSize = retentionBuffer.getByteBuffer().position();
            newBufferChunkRetentionSize = (int) (retentionBuffer.getByteBuffer().capacity() * config.getChunkExpandRatio());
        }

        while (newBufferChunkRetentionSize < (writeSize + existingDataSize)) {
            newBufferChunkRetentionSize *= config.getChunkExpandRatio();
        }

        ByteBuffer acquiredBuffer = bufferPool.acquireBuffer(newBufferChunkRetentionSize);
        if (acquiredBuffer == null) {
            throw new BufferFullException("Buffer is full. config=" + config + ", bufferPool=" + bufferPool);
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

    private void loadDataToRetentionBuffers(String tag, ByteBuffer src)
            throws IOException
    {
        retentionBuffers.lock(tag);
        try {
            RetentionBuffer buffer = prepareBuffer(tag, src.remaining());
            buffer.getByteBuffer().put(src);
            buffer.getLastUpdatedTimeMillis().set(System.currentTimeMillis());
            moveRetentionBufferIfNeeded(tag, buffer);
        }
        finally {
            retentionBuffers.unlock(tag);
        }
    }

    @Override
    protected void loadBufferFromFile(List<String> params, FileChannel channel)
    {
        if (params.size() != 1) {
            throw new IllegalArgumentException("The number of params should be 1: params=" + params);
        }
        String tag = params.get(0);

        try {
            MappedByteBuffer src = channel.map(FileChannel.MapMode.PRIVATE, 0, channel.size());
            loadDataToRetentionBuffers(tag, src);
        }
        catch (Exception e) {
            LOG.error("Failed to load data to flushableBuffers: params={}, channel={}", params, channel);
        }
    }

    private void saveBuffer(TaggableBuffer buffer)
    {
        saveBuffer(Collections.singletonList(buffer.getTag()), buffer.getByteBuffer());
    }

    @Override
    protected void saveAllBuffersToFile()
            throws IOException
    {
        moveRetentionBuffersToFlushable(true);  // Just in case

        TaggableBuffer flushableBuffer;
        while ((flushableBuffer = flushableBuffers.poll()) != null) {
            saveBuffer(flushableBuffer);
        }
        while ((flushableBuffer = backupBuffers.poll()) != null) {
            saveBuffer(flushableBuffer);
        }
    }

    @Override
    public void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(timestamp, data));
        outputStream.close();

        loadDataToRetentionBuffers(tag, ByteBuffer.wrap(outputStream.toByteArray()));
    }

    private void moveRetentionBufferIfNeeded(String tag, RetentionBuffer buffer)
            throws IOException
    {
        if (buffer.getByteBuffer().position() > config.getChunkRetentionSize()) {
            moveRetentionBufferToFlushable(tag, buffer);
        }
    }

    private void moveRetentionBuffersToFlushable(final boolean force)
            throws IOException
    {
        final long expiredThreshold = System.currentTimeMillis() - config.getChunkRetentionTimeMillis();

        retentionBuffers.foreachWithException(
                new RetentionBuffers.ThrowableIterateAction<IOException>() {
                    @Override
                    public void iterateWithException(String tag, RetentionBuffer buffer)
                            throws IOException
                    {
                        if (buffer != null) {
                            if (force || buffer.getLastUpdatedTimeMillis().get() < expiredThreshold) {
                                moveRetentionBufferToFlushable(tag, buffer);
                            }
                        }
                    }
                }
        , true);
    }

    private void moveRetentionBufferToFlushable(String tag, RetentionBuffer buffer)
            throws IOException
    {
        try {
            LOG.trace("moveRetentionBufferToFlushable(): tag={}, buffer={}", tag, buffer);
            buffer.getByteBuffer().flip();
            flushableBuffers.put(new TaggableBuffer(tag, buffer.getByteBuffer()));
            retentionBuffers.remove(tag);
        }
        catch (InterruptedException e) {
            throw new IOException("Failed to move retention buffer due to interruption", e);
        }
    }

    @Override
    public String bufferFormatType()
    {
        return FORMAT_TYPE;
    }

    @Override
    public void flushInternal(Sender sender, boolean force)
            throws IOException
    {
        moveRetentionBuffersToFlushable(force);

        TaggableBuffer flushableBuffer;
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        MessagePacker messagePacker = MessagePack.newDefaultPacker(header);
        while (!Thread.currentThread().isInterrupted() &&
                (flushableBuffer = flushableBuffers.poll()) != null) {
            boolean keepBuffer = false;
            try {
                LOG.trace("flushInternal(): bufferUsage={}, flushableBuffer={}", getBufferUsage(), flushableBuffer);
                String tag = flushableBuffer.getTag();
                ByteBuffer byteBuffer = flushableBuffer.getByteBuffer();
                if (config.isAckResponseMode()) {
                    messagePacker.packArrayHeader(3);
                }
                else {
                    messagePacker.packArrayHeader(2);
                }
                messagePacker.packString(tag);
                messagePacker.packRawStringHeader(byteBuffer.limit());
                messagePacker.flush();

                try {
                    ByteBuffer headerBuffer = ByteBuffer.wrap(header.toByteArray());
                    List<ByteBuffer> dataList = Arrays.asList(headerBuffer, byteBuffer);
                    if (config.isAckResponseMode()) {
                        String uuid = UUID.randomUUID().toString();
                        byte[] uuidBytes = uuid.getBytes(CHARSET);
                        synchronized (sender) {
                            sender.sendWithAck(dataList, uuidBytes);
                        }
                    } else {
                        synchronized (sender) {
                            sender.send(dataList);
                        }
                    }
                }
                catch (IOException e) {
                    LOG.warn("Failed to send data. The data is going to be saved into the buffer again: data={}", flushableBuffer);
                    keepBuffer = true;
                    throw e;
                }
            }
            finally {
                header.reset();
                if (keepBuffer) {
                    try {
                        flushableBuffers.put(flushableBuffer);
                    }
                    catch (InterruptedException e1) {
                        LOG.warn("Failed to save the data into the buffer. Trying to save it in extra buffer: chunk={}", flushableBuffer);
                        backupBuffers.add(flushableBuffer);
                    }
                }
                else {
                    bufferPool.returnBuffer(flushableBuffer.getByteBuffer());
                }
            }
        }
    }

    @Override
    protected synchronized void closeInternal()
    {
        retentionBuffers.clear();
        bufferPool.releaseBuffers();
    }

    @Override
    public long getAllocatedSize()
    {
        return bufferPool.getAllocatedSize();
    }

    @Override
    public long getBufferedDataSize()
    {
        final AtomicLong size = new AtomicLong();
        retentionBuffers.foreach(new RetentionBuffers.IterateAction() {
            @Override
            public void iterate(String tag, RetentionBuffer buffer)
            {
                if (buffer != null && buffer.getByteBuffer() != null) {
                    size.addAndGet(buffer.getByteBuffer().position());
                }
            }
        }, false);

        for (TaggableBuffer buffer : flushableBuffers) {
            if (buffer.getByteBuffer() != null) {
                size.addAndGet(buffer.getByteBuffer().remaining());
            }
        }
        return size.get();
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
            return "RetentionBuffer{" +
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

    public int getChunkInitialSize()
    {
        return config.getChunkInitialSize();
    }

    public float getChunkExpandRatio()
    {
        return config.getChunkExpandRatio();
    }

    public int getChunkRetentionSize()
    {
        return config.getChunkRetentionSize();
    }

    public int getChunkRetentionTimeMillis()
    {
        return config.getChunkRetentionTimeMillis();
    }

    @Override
    public String toString()
    {
        return "PackedForwardBuffer{" +
                "retentionBuffers=" + retentionBuffers +
                ", flushableBuffers=" + flushableBuffers +
                ", backupBuffers=" + backupBuffers +
                ", bufferPool=" + bufferPool +
                ", config=" + config +
                "} " + super.toString();
    }

    public static class Config
        implements Buffer.Instantiator
    {
        private Buffer.Config baseConfig = new Buffer.Config();
        private int chunkInitialSize = 1024 * 1024;
        private float chunkExpandRatio = 2.0f;
        private int chunkRetentionSize = 4 * 1024 * 1024;
        private int chunkRetentionTimeMillis = 400;

        public Buffer.Config getBaseConfig()
        {
            return baseConfig;
        }

        public long getMaxBufferSize()
        {
            return baseConfig.getMaxBufferSize();
        }

        public Config setMaxBufferSize(long maxBufferSize)
        {
            baseConfig.setMaxBufferSize(maxBufferSize);
            return this;
        }

        public Config setFileBackupPrefix(String fileBackupPrefix)
        {
            baseConfig.setFileBackupPrefix(fileBackupPrefix);
            return this;
        }

        public Config setFileBackupDir(String fileBackupDir)
        {
            baseConfig.setFileBackupDir(fileBackupDir);
            return this;
        }

        public Config setAckResponseMode(boolean ackResponseMode)
        {
            baseConfig.setAckResponseMode(ackResponseMode);
            return this;
        }

        public boolean isAckResponseMode()
        {
            return baseConfig.isAckResponseMode();
        }

        public List<Module> getJacksonModules()
        {
            return baseConfig.getJacksonModules();
        }

        public String getFileBackupPrefix()
        {
            return baseConfig.getFileBackupPrefix();
        }

        public String getFileBackupDir()
        {
            return baseConfig.getFileBackupDir();
        }

        public Config setJacksonModules(List<Module> jacksonModules)
        {
            baseConfig.setJacksonModules(jacksonModules);
            return this;
        }

        public int getChunkInitialSize()
        {
            return chunkInitialSize;
        }

        public Config setChunkInitialSize(int chunkInitialSize)
        {
            this.chunkInitialSize = chunkInitialSize;
            return this;
        }

        public float getChunkExpandRatio()
        {
            return chunkExpandRatio;
        }

        public Config setChunkExpandRatio(float chunkExpandRatio)
        {
            this.chunkExpandRatio = chunkExpandRatio;
            return this;
        }

        public int getChunkRetentionSize()
        {
            return chunkRetentionSize;
        }

        public Config setChunkRetentionSize(int chunkRetentionSize)
        {
            this.chunkRetentionSize = chunkRetentionSize;
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
                    "baseConfig=" + baseConfig +
                    ", chunkInitialSize=" + chunkInitialSize +
                    ", chunkExpandRatio=" + chunkExpandRatio +
                    ", chunkRetentionSize=" + chunkRetentionSize +
                    ", chunkRetentionTimeMillis=" + chunkRetentionTimeMillis +
                    '}';
        }

        protected PackedForwardBuffer createInstanceInternal()
        {
            return new PackedForwardBuffer(this);
        }

        @Override
        public PackedForwardBuffer createInstance()
        {
            PackedForwardBuffer buffer = new PackedForwardBuffer(this);
            buffer.init();
            return buffer;
        }
    }
}
