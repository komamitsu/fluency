/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.buffer;

import org.msgpack.core.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

class BufferPool
{
    @VisibleForTesting
    final Map<Integer, LinkedBlockingQueue<ByteBuffer>> bufferPool = new HashMap<>();
    private final AtomicLong allocatedSize = new AtomicLong();
    private final int initialBufferSize;
    private final long maxBufferSize;
    private final boolean jvmHeapBufferMode;

    public BufferPool(int initialBufferSize, long maxBufferSize)
    {
        this(initialBufferSize, maxBufferSize, false);
    }

    public BufferPool(int initialBufferSize, long maxBufferSize, boolean jvmHeapBufferMode)
    {
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.jvmHeapBufferMode = jvmHeapBufferMode;
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
                buffers = new LinkedBlockingQueue<>();
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
                releaseBuffers();
                return null;    // `null` means the buffer is full.
            }
            if (currentAllocatedSize == allocatedSize.getAndAdd(normalizedBufferSize)) {
                ByteBuffer buf;
                if (jvmHeapBufferMode) {
                    buf = ByteBuffer.allocate(normalizedBufferSize);
                }
                else {
                    buf = ByteBuffer.allocateDirect(normalizedBufferSize);
                }
                return buf;
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

    public void releaseBuffers()
    {
        // TODO: Stop releasing all the buffers when having released buffers to some extent
        synchronized (bufferPool) {
            for (Map.Entry<Integer, LinkedBlockingQueue<ByteBuffer>> entry : bufferPool.entrySet()) {
                ByteBuffer buffer;
                while ((buffer = entry.getValue().poll()) != null) {
                    allocatedSize.addAndGet(-buffer.capacity());
                }
            }
        }
    }

    public boolean getJvmHeapBufferMode()
    {
        return jvmHeapBufferMode;
    }

    @Override
    public String toString()
    {
        return "BufferPool{" +
                "bufferPool=" + bufferPool +
                ", allocatedSize=" + allocatedSize +
                ", initialBufferSize=" + initialBufferSize +
                ", maxBufferSize=" + maxBufferSize +
                ", jvmHeapBufferMode=" + jvmHeapBufferMode +
                '}';
    }
}
