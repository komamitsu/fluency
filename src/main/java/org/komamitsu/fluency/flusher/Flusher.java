package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
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
    protected final Sender sender;
    private final Config config;

    protected Flusher(Buffer buffer, Sender sender, Config config)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.config = config;
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
            try {
                sender.close();
            }
            catch (Exception e) {
                LOG.error("Failed to close the sender", e);
            }
            finally {
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<Void> future = executor.submit(new Callable<Void>()
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
                } finally {
                    executor.shutdown();
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

    public Sender getSender()
    {
        return sender;
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
                ", sender=" + sender +
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
        Flusher createInstance(Buffer buffer, Sender sender);
    }
}
