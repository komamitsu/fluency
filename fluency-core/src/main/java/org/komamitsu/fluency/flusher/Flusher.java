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

import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Flusher
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
    protected final Buffer buffer;
    protected final Ingester ingester;
    private final AtomicBoolean isTerminated = new AtomicBoolean();
    private final Config config;
    private final BlockingQueue<Boolean> eventQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public Flusher(Config config, Buffer buffer, Ingester ingester)
    {
        config.validate();
        this.config = config;
        this.buffer = buffer;
        this.ingester = ingester;
        executorService.execute(this::runLoop);
    }

    private void runLoop() {
        Boolean wakeup = null;
        do {
            try {
                wakeup = eventQueue.poll(config.getFlushIntervalMillis(), TimeUnit.MILLISECONDS);
                boolean force = wakeup != null;
                buffer.flush(ingester, force);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Throwable e) {
                LOG.error("Failed to flush", e);
            }
        }
        while (!executorService.isShutdown());

        if (wakeup == null) {
            // The above run loop can quit without force buffer flush in the following cases
            // - close() is called right after the repeated non-force buffer flush executed in the run loop
            //
            // In these cases, remaining buffers wont't be flushed.
            // So force buffer flush is executed here just in case
            try {
                buffer.flush(ingester, true);
            }
            catch (Throwable e) {
                LOG.error("Failed to flush", e);
            }
        }
    }

    public Buffer getBuffer()
    {
        return buffer;
    }

    @Override
    public void flush()
    {
        try {
            eventQueue.put(true);
        }
        catch (InterruptedException e) {
            LOG.warn("Failed to force flushing buffer", e);
            Thread.currentThread().interrupt();
        }
    }

    private void flushBufferQuietly()
    {
        LOG.trace("Flushing the buffer");

        try {
            flush();
        }
        catch (Throwable e) {
            LOG.error("Failed to call flush()", e);
        }
    }

    private void finishExecutorQuietly()
    {
        LOG.trace("Finishing the executor");

        ExecutorServiceUtils.finishExecutorService(
                executorService,
                config.getWaitUntilBufferFlushed());
    }

    private void closeBufferQuietly()
    {
        LOG.trace("Closing the buffer");

        try {
            buffer.close();
        }
        catch (Throwable e) {
            LOG.warn("Failed to close the buffer", e);
        }
        isTerminated.set(true);
    }

    private void closeIngesterQuietly()
    {
        LOG.trace("Closing the ingester");

        try {
            // Close the socket at the end to prevent the server from failing to read from the connection
            ingester.close();
        }
        catch (Throwable e) {
            LOG.error("Failed to close the ingester", e);
        }
    }

    @Override
    public void close()
    {
        flushBufferQuietly();

        finishExecutorQuietly();

        closeBufferQuietly();

        closeIngesterQuietly();
    }

    public boolean isTerminated()
    {
        return isTerminated.get();
    }

    public Ingester getIngester()
    {
        return ingester;
    }

    public int getFlushIntervalMillis()
    {
        return config.getFlushIntervalMillis();
    }

    public int getWaitUntilBufferFlushed()
    {
        return config.getWaitUntilBufferFlushed();
    }

    public int getWaitUntilTerminated()
    {
        return config.getWaitUntilTerminated();
    }

    @Override
    public String toString()
    {
        return "Flusher{" +
                "isTerminated=" + isTerminated +
                ", buffer=" + buffer +
                ", ingester=" + ingester +
                ", config=" + config +
                '}';
    }

    public static class Config
    {
        private static final int MIN_FLUSH_INTERVAL_MILLIS = 20;
        private static final int MAX_FLUSH_INTERVAL_MILLIS = 2000;
        private static final int MIN_WAIT_UNTIL_BUFFER_FLUSHED = 1;
        private static final int MIN_WAIT_UNTIL_TERMINATED = 1;
        private int flushIntervalMillis = 600;
        private int waitUntilBufferFlushed = 60;
        private int waitUntilTerminated = 60;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public void setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
        }

        public int getWaitUntilBufferFlushed()
        {
            return waitUntilBufferFlushed;
        }

        public void setWaitUntilBufferFlushed(int waitUntilBufferFlushed)
        {
            this.waitUntilBufferFlushed = waitUntilBufferFlushed;
        }

        public int getWaitUntilTerminated()
        {
            return waitUntilTerminated;
        }

        public void setWaitUntilTerminated(int waitUntilTerminated)
        {
            this.waitUntilTerminated = waitUntilTerminated;
        }

        void validate()
        {
            if (flushIntervalMillis < MIN_FLUSH_INTERVAL_MILLIS || flushIntervalMillis > MAX_FLUSH_INTERVAL_MILLIS) {
                throw new IllegalArgumentException(
                        String.format(
                                "Flush Interval (%d ms) should be between %d and %d. If you want to increase the retention time of buffered data, adjust Buffer Chunk Retention Time",
                                flushIntervalMillis, MIN_FLUSH_INTERVAL_MILLIS, MAX_FLUSH_INTERVAL_MILLIS));
            }

            if (waitUntilBufferFlushed < MIN_WAIT_UNTIL_BUFFER_FLUSHED) {
                throw new IllegalArgumentException(
                        String.format(
                                "Wait Until Buffer Flushed (%d seconds) should be %d seconds or more",
                                waitUntilBufferFlushed, MIN_WAIT_UNTIL_BUFFER_FLUSHED));
            }

            if (waitUntilTerminated < MIN_WAIT_UNTIL_TERMINATED) {
                throw new IllegalArgumentException(
                        String.format(
                                "Wait Until Terminated (%d seconds) should be %d seconds or more",
                                waitUntilTerminated, MIN_WAIT_UNTIL_TERMINATED));
            }
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "flushIntervalMillis=" + flushIntervalMillis +
                    ", waitUntilBufferFlushed=" + waitUntilBufferFlushed +
                    ", waitUntilTerminated=" + waitUntilTerminated +
                    '}';
        }
    }
}
