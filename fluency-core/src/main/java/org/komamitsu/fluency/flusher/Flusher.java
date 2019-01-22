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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Flusher
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
    private final AtomicBoolean isTerminated = new AtomicBoolean();
    protected final Buffer buffer;
    protected final Ingester ingester;
    private final Config config;

    protected Flusher(Config config, Buffer buffer, Ingester ingester)
    {
        this.config = config;
        this.buffer = buffer;
        this.ingester = ingester;
    }

    public Buffer getBuffer()
    {
        return buffer;
    }

    protected abstract void flushInternal(boolean force)
            throws IOException;

    protected abstract void beforeClosingBuffer()
            throws IOException;

    public void onUpdate()
            throws IOException
    {
        flushInternal(false);
    }

    @Override
    public void flush()
            throws IOException
    {
        flushInternal(true);
    }

    @Override
    public void close()
    {
        try {
            beforeClosingBuffer();
        }
        catch (Exception e) {
            LOG.error("Failed to call beforeClosingBuffer()", e);
        }
        finally {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Void> future = executorService.submit(() -> {
                closeBuffer();
                isTerminated.set(true);
                return null;
            });

            try {
                future.get(config.getWaitUntilTerminated(), TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                LOG.warn("closeBuffer() failed", e);
            }
            catch (TimeoutException e) {
                LOG.warn("closeBuffer() timed out", e);
            }
            finally {
                try {
                    executorService.shutdown();
                }
                finally {
                    try {
                        // Close the socket at the end to prevent the server from failing to read from the connection
                        ingester.close();
                    }
                    catch (Exception e) {
                        LOG.error("Failed to close the sender", e);
                    }

                }
            }
        }
    }

    public boolean isTerminated()
    {
        return isTerminated.get();
    }

    private void closeBuffer()
    {
        LOG.trace("closeBuffer(): closing buffer");
        buffer.close();
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

    public abstract static class Config
    {
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
