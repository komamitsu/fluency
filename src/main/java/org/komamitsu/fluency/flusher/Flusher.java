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
import org.komamitsu.fluency.transporter.Transporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.Callable;
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
    protected final Transporter transporter;
    private final Config config;

    protected Flusher(Config config, Buffer buffer, Transporter transporter)
    {
        this.config = config;
        this.buffer = buffer;
        this.transporter = transporter;
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
            throws IOException
    {
        try {
            beforeClosingBuffer();
        }
        catch (Exception e) {
            LOG.error("Failed to call beforeClosingBuffer()", e);
        }
        finally {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Void> future = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    closeBuffer();
                    isTerminated.set(true);
                    return null;
                }
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
                        transporter.close();
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

    public Transporter getTransporter()
    {
        return transporter;
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
                ", transporter=" + transporter +
                ", config=" + config +
                '}';
    }

    public static class Config
    {
        private int flushIntervalMillis = 600;
        private int waitUntilBufferFlushed = 60;
        private int waitUntilTerminated = 60;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public Config setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return this;
        }

        public int getWaitUntilBufferFlushed()
        {
            return waitUntilBufferFlushed;
        }

        public Config setWaitUntilBufferFlushed(int waitUntilBufferFlushed)
        {
            this.waitUntilBufferFlushed = waitUntilBufferFlushed;
            return this;
        }

        public int getWaitUntilTerminated()
        {
            return waitUntilTerminated;
        }

        public Config setWaitUntilTerminated(int waitUntilTerminated)
        {
            this.waitUntilTerminated = waitUntilTerminated;
            return this;
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

    public interface Instantiator
    {
        Flusher createInstance(Buffer buffer, Transporter transporter);
    }
}
