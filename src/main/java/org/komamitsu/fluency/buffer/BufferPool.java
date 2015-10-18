package org.komamitsu.fluency.buffer;

import org.msgpack.core.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BufferPool
{
    private static final Comparator<Integer> INT_REV_COMPARATOR = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2)
        {
            return o2 - o1;
        }
    };

    @VisibleForTesting
    final Map<Integer, LinkedBlockingQueue<ByteBuffer>> bufferPool = new HashMap<Integer, LinkedBlockingQueue<ByteBuffer>>();
    private final AtomicLong allocatedSize = new AtomicLong();
    private final int initialBufferSize;
    private final int maxBufferSize;

    public BufferPool(int initialBufferSize, int maxBufferSize)
    {
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
    }

    public ByteBuffer acquireBuffer(int bufferSize)
    {
        int normalizedBufferSize = initialBufferSize;
        while (normalizedBufferSize < bufferSize) {
            normalizedBufferSize *= 2;
        }

        LinkedBlockingQueue<ByteBuffer> buffers;
        synchronized (bufferPool) {
            buffers = bufferPool.get(normalizedBufferSize);
            if (buffers == null) {
                buffers = new LinkedBlockingQueue<ByteBuffer>();
                bufferPool.put(normalizedBufferSize, buffers);
            }
        }

        ByteBuffer buffer = buffers.poll();
        if (buffer != null) {
            return buffer;
        }

        /*
        synchronized (allocatedSize) {
            if (allocatedSize.get() + normalizedBufferSize > maxBufferSize) {
                return null;    // `null` means the buffer is full.
            }
            allocatedSize.addAndGet(normalizedBufferSize);
            return ByteBuffer.allocateDirect(normalizedBufferSize);
        }
        */

        while (true) {
            long currentAllocatedSize = allocatedSize.get();
            if (currentAllocatedSize + normalizedBufferSize > maxBufferSize) {
                releaseBuffers(0.5f);
                return null;    // `null` means the buffer is full.
            }
            if (currentAllocatedSize == allocatedSize.getAndAdd(normalizedBufferSize)) {
                return ByteBuffer.allocateDirect(normalizedBufferSize);
            }
            allocatedSize.getAndAdd(-normalizedBufferSize);
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer)
    {
        LinkedBlockingQueue<ByteBuffer> buffers = bufferPool.get(byteBuffer.capacity());
        if (buffers == null) {
            throw new IllegalStateException("`buffers` shouldn't be null");
        }

        byteBuffer.position(0);
        byteBuffer.limit(byteBuffer.capacity());
        buffers.offer(byteBuffer);
    }

    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    public void releaseBuffers(float releaseRatio)
    {
        List<Integer> sortedSizes = Arrays.asList(bufferPool.keySet().toArray(new Integer[bufferPool.keySet().size()]));
        Collections.sort(sortedSizes, INT_REV_COMPARATOR);

        while (true) {
            boolean released = false;
            for (Integer size : sortedSizes) {
                LinkedBlockingQueue<ByteBuffer> buffers = bufferPool.get(size);
                ByteBuffer buffer;
                if ((buffer = buffers.poll()) != null) {
                    released = true;
                    long currentAllocatedSize = allocatedSize.addAndGet(-buffer.capacity());
                    if (currentAllocatedSize <= maxBufferSize * releaseRatio) {
                        return;
                    }
                }
            }
            if (!released) {
                return;
            }
        }
    }

    @Override
    public String toString()
    {
        return "BufferPool{" +
                "bufferPool=" + bufferPool +
                ", allocatedSize=" + allocatedSize +
                ", initialBufferSize=" + initialBufferSize +
                ", maxBufferSize=" + maxBufferSize +
                '}';
    }
}
