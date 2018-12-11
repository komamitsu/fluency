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

import com.fasterxml.jackson.databind.Module;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

// TODO: Maybe this test isn't needed
public class TestableBuffer
        extends Buffer
{
    private static final Logger LOG = LoggerFactory.getLogger(TestableBuffer.class);
    public static final int RECORD_DATA_SIZE = 100;
    public static final int ALLOC_SIZE = 128;
    private final List<Tuple3<String, Long, Map<String, Object>>> events = new ArrayList<Tuple3<String, Long, Map<String, Object>>>();
    private final AtomicInteger flushCount = new AtomicInteger();
    private final AtomicInteger forceFlushCount = new AtomicInteger();
    private final AtomicInteger closeCount = new AtomicInteger();
    private final AtomicInteger bufferedDataSize = new AtomicInteger();
    private final AtomicInteger allocatedSize = new AtomicInteger();
    private final List<Tuple<List<String>, ByteBuffer>> savableBuffers = new ArrayList<Tuple<List<String>, ByteBuffer>>();
    private final List<Tuple<List<String>, ByteBuffer>> loadedBuffers = new ArrayList<Tuple<List<String>, ByteBuffer>>();
    private final Config config;

    private TestableBuffer(Config config)
    {
        super(config.getBaseConfig(), mock(RecordFormatter.class));
        this.config = config;
    }

    public void setSavableBuffer(List<String> params, ByteBuffer buffer)
    {
        savableBuffers.add(new Tuple<>(params, buffer));
    }

    @Override
    public void append(String tag, EventTime timestamp, Map<String, Object> data)
    {
        throw new IllegalStateException("Shouldn't be called");
    }

    @Override
    public void appendMessagePackMapValue(String tag, long timestamp, byte[] mapValue, int offset, int len)
    {
        throw new IllegalStateException("Shouldn't be called");
    }

    @Override
    public void appendMessagePackMapValue(String tag, EventTime timestamp, byte[] mapValue, int offset, int len)
    {
        throw new IllegalStateException("Shouldn't be called");
    }

    @Override
    public void appendMessagePackMapValue(String tag, long timestamp, ByteBuffer mapValue)
    {
        throw new IllegalStateException("Shouldn't be called");
    }

    @Override
    public void appendMessagePackMapValue(String tag, EventTime timestamp, ByteBuffer mapValue)
    {
        throw new IllegalStateException("Shouldn't be called");
    }

    @Override
    protected void loadBufferFromFile(List<String> params, FileChannel channel)
    {
        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.PRIVATE, 0, channel.size());
            loadedBuffers.add(new Tuple<>(params, buffer));
        }
        catch (IOException e) {
            LOG.warn("Failed to map the FileChannel: channel=" + channel, e);
        }
    }

    @Override
    protected void saveAllBuffersToFile()
    {
        for (Tuple<List<String>, ByteBuffer> savableBuffer : savableBuffers) {
            saveBuffer(savableBuffer.getFirst(), savableBuffer.getSecond());
        }
    }

    @Override
    public void append(String tag, long timestamp, Map data)
    {
        events.add(new Tuple3<String, Long, Map<String, Object>>(tag, timestamp, data));
        allocatedSize.addAndGet(ALLOC_SIZE);
        bufferedDataSize.addAndGet(RECORD_DATA_SIZE);
    }

    @Override
    public void flushInternal(Ingester ingester, boolean force)
    {
        if (force) {
            forceFlushCount.incrementAndGet();
        }
        else {
            flushCount.incrementAndGet();
        }
        allocatedSize.set(0);
        bufferedDataSize.set(0);
    }

    @Override
    public String bufferFormatType()
    {
        return "test";
    }

    @Override
    protected void closeInternal()
    {
        if (config.getWaitBeforeCloseMillis() > 0) {
            long start = System.currentTimeMillis();
            try {
                TimeUnit.MILLISECONDS.sleep(config.getWaitBeforeCloseMillis());
            }
            catch (InterruptedException e) {
                long rest = config.getWaitBeforeCloseMillis() - (System.currentTimeMillis() - start);
                if (rest > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(rest);
                    }
                    catch (InterruptedException e1) {
                    }
                }
            }
        }

        closeCount.incrementAndGet();
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    @Override
    public long getBufferedDataSize()
    {
        return bufferedDataSize.get();
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
        implements Buffer.Instantiator
    {
        private final Buffer.Config baseConfig = new Buffer.Config();
        private int waitBeforeCloseMillis;

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

        public String getFileBackupPrefix()
        {
            return baseConfig.getFileBackupPrefix();
        }

        public String getFileBackupDir()
        {
            return baseConfig.getFileBackupDir();
        }

        public int getWaitBeforeCloseMillis()
        {
            return waitBeforeCloseMillis;
        }

        public Config setWaitBeforeCloseMillis(int wait)
        {
            this.waitBeforeCloseMillis = wait;
            return this;
        }

        @Override
        public TestableBuffer createInstance(RecordFormatter recordFormatter)
        {
            TestableBuffer buffer = new TestableBuffer(this);
            buffer.init();
            return buffer;
        }
    }
}
