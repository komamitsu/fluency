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

package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.util.ExecutorServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class SyncFlusher
        extends Flusher
{
    private static final Logger LOG = LoggerFactory.getLogger(SyncFlusher.class);
    private final AtomicLong lastFlushTimeMillis = new AtomicLong();
    private final Config config;

    private SyncFlusher(Config config, Buffer buffer, Ingester ingester)
    {
        super(config.getBaseConfig(), buffer, ingester);
        this.config = config;
        lastFlushTimeMillis.set(System.currentTimeMillis());
    }

    @Override
    protected void flushInternal(boolean force)
            throws IOException
    {
        long now = System.currentTimeMillis();
        if (force ||
                now > lastFlushTimeMillis.get() + config.getFlushIntervalMillis() ||
                buffer.getBufferUsage() > config.getBufferOccupancyThreshold()) {
            buffer.flush(ingester, force);
            lastFlushTimeMillis.set(now);
        }
    }

    @Override
    protected void beforeClosingBuffer()
            throws IOException
    {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(new Callable<Void>()
        {
            @Override
            public Void call()
                    throws Exception
            {
                flushInternal(true);
                return null;
            }
        });
        try {
            future.get(config.getWaitUntilBufferFlushed(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("Interrupted", e);
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
            LOG.warn("flushInternal() failed", e);
        }
        catch (TimeoutException e) {
            LOG.warn("flushInternal() timed out", e);
        }
        finally {
            ExecutorServiceUtils.finishExecutorService(executorService);
        }
    }

    public float getBufferOccupancyThreshold()
    {
        return config.getBufferOccupancyThreshold();
    }

    @Override
    public String toString()
    {
        return "SyncFlusher{" +
                "lastFlushTimeMillis=" + lastFlushTimeMillis +
                ", config=" + config +
                "} " + super.toString();
    }

    public static class Config
        implements Flusher.Instantiator
    {
        private final Flusher.Config baseConfig = new Flusher.Config();

        private float bufferOccupancyThreshold = 0.6f;

        public Flusher.Config getBaseConfig()
        {
            return baseConfig;
        }

        public int getFlushIntervalMillis()
        {
            return baseConfig.getFlushIntervalMillis();
        }

        public Config setFlushIntervalMillis(int flushIntervalMillis)
        {
            baseConfig.setFlushIntervalMillis(flushIntervalMillis);
            return this;
        }

        public int getWaitUntilBufferFlushed()
        {
            return baseConfig.getWaitUntilBufferFlushed();
        }

        public Config setWaitUntilBufferFlushed(int wait)
        {
            baseConfig.setWaitUntilBufferFlushed(wait);
            return this;
        }

        public int getWaitUntilTerminated()
        {
            return baseConfig.getWaitUntilTerminated();
        }

        public Config setWaitUntilTerminated(int wait)
        {
            baseConfig.setWaitUntilTerminated(wait);
            return this;
        }

        public float getBufferOccupancyThreshold()
        {
            return bufferOccupancyThreshold;
        }

        public Config setBufferOccupancyThreshold(float bufferOccupancyThreshold)
        {
            this.bufferOccupancyThreshold = bufferOccupancyThreshold;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", bufferOccupancyThreshold=" + bufferOccupancyThreshold +
                    '}';
        }

        @Override
        public SyncFlusher createInstance(Buffer buffer, Ingester ingester)
        {
            return new SyncFlusher(this, buffer, ingester);
        }
    }
}
