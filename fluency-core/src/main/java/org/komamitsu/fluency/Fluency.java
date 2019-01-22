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

package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.Flusher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Fluency
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Fluency.class);
    private final Buffer buffer;
    private final Flusher flusher;
    private final Emitter emitter = new Emitter();

    // Fluency has this public constructor, but using FluentBuilder is usually more recommended.
    public Fluency(Buffer buffer, Flusher flusher)
    {
        this.buffer = buffer;
        this.flusher = flusher;
    }

    public void emit(final String tag, final long timestamp, final Map<String, Object> data)
            throws IOException
    {
        emitter.emit(() -> buffer.append(tag, timestamp, data));
    }

    public void emit(String tag, Map<String, Object> data)
            throws IOException
    {
        emit(tag, System.currentTimeMillis() / 1000, data);
    }

    public void emit(final String tag, final EventTime eventTime, final Map<String, Object> data)
            throws IOException
    {
        emitter.emit(() -> buffer.append(tag, eventTime, data));
    }

    public void emit(final String tag, final long timestamp, final byte[] mapValue, final int offset, final int len)
            throws IOException
    {
        emitter.emit(() -> buffer.appendMessagePackMapValue(tag, timestamp, mapValue, offset, len));
    }

    public void emit(String tag, byte[] mapValue, int offset, int len)
            throws IOException
    {
        emit(tag, System.currentTimeMillis() / 1000, mapValue, offset, len);
    }

    public void emit(final String tag, final EventTime eventTime, final byte[] mapValue, final int offset, final int len)
            throws IOException
    {
        emitter.emit(() -> buffer.appendMessagePackMapValue(tag, eventTime, mapValue, offset, len));
    }

    public void emit(final String tag, final long timestamp, final ByteBuffer mapValue)
            throws IOException
    {
        emitter.emit(() -> buffer.appendMessagePackMapValue(tag, timestamp, mapValue));
    }

    public void emit(String tag, ByteBuffer mapValue)
            throws IOException
    {
        emit(tag, System.currentTimeMillis() / 1000, mapValue);
    }

    public void emit(final String tag, final EventTime eventTime, final ByteBuffer mapValue)
            throws IOException
    {
        emitter.emit(() -> buffer.appendMessagePackMapValue(tag, eventTime, mapValue));
    }

    @Override
    public void flush()
            throws IOException
    {
        flusher.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flusher.close();
    }

    public void clearBackupFiles()
    {
        buffer.clearBackupFiles();
    }

    public long getAllocatedBufferSize()
    {
        return buffer.getAllocatedSize();
    }

    public long getBufferedDataSize()
    {
        return buffer.getBufferedDataSize();
    }

    public boolean isTerminated()
    {
        return flusher.isTerminated();
    }

    public boolean waitUntilAllBufferFlushed(int maxWaitSeconds)
            throws InterruptedException
    {
        int intervalMilli = 500;
        for (int i = 0; i < maxWaitSeconds * (1000 / intervalMilli); i++) {
            long bufferedDataSize = getBufferedDataSize();
            LOG.debug("Waiting for flushing all buffer: {}", bufferedDataSize);
            if (getBufferedDataSize() == 0) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(intervalMilli);
        }
        LOG.warn("Buffered data still remains: {}", getBufferedDataSize());
        return false;
    }

    public boolean waitUntilFlusherTerminated(int maxWaitSeconds)
            throws InterruptedException
    {
        int intervalMilli = 500;
        for (int i = 0; i < maxWaitSeconds * (1000 / intervalMilli); i++) {
            boolean terminated = isTerminated();
            LOG.debug("Waiting until the flusher is terminated: {}", terminated);
            if (terminated) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(intervalMilli);
        }
        LOG.warn("The flusher isn't terminated");
        return false;
    }

    public Buffer getBuffer()
    {
        return buffer;
    }

    public Flusher getFlusher()
    {
        return flusher;
    }

    @Override
    public String toString()
    {
        return "Fluency{" +
                "buffer=" + buffer +
                ", flusher=" + flusher +
                '}';
    }

    private interface Append
    {
        void append()
                throws IOException;
    }

    private class Emitter
    {
        void emit(Append appender)
                throws IOException
        {
            try {
                appender.append();
                flusher.onUpdate();
            }
            catch (BufferFullException e) {
                LOG.error("emit() failed due to buffer full. Flushing buffer. Please try again...");
                flusher.flush();
                throw e;
            }
        }
    }
}
