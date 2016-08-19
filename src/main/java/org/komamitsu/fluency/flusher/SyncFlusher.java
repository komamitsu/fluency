package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SyncFlusher
        extends Flusher
{
    private static final Logger LOG = LoggerFactory.getLogger(SyncFlusher.class);
    private final AtomicLong lastFlushTimeMillis = new AtomicLong();
    private final AtomicBoolean isTerminated = new AtomicBoolean();

    private SyncFlusher(Buffer buffer, Sender sender, SyncFlusher.Config flusherConfig)
    {
        super(buffer, sender, flusherConfig);
    }

    protected SyncFlusher.Config getConfig()
    {
        return (SyncFlusher.Config) flusherConfig;
    }

    @Override
    protected void flushInternal(boolean force)
            throws IOException
    {
        long now = System.currentTimeMillis();
        if (force ||
                now > lastFlushTimeMillis.get() + getConfig().getFlushIntervalMillis() ||
                buffer.getBufferUsage() > getConfig().getBufferOccupancyThreshold()) {
            buffer.flush(sender, force);
            lastFlushTimeMillis.set(now);
        }
    }

    @Override
    protected void closeInternal()
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
            future.get(getConfig().getWaitAfterClose(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("Interrupted", e);
        }
        catch (ExecutionException e) {
            LOG.warn("flushInternal() failed", e);
        }
        catch (TimeoutException e) {
            LOG.warn("flushInternal() timed out", e);
        }
        closeBuffer();
        isTerminated.set(true);
    }

    @Override
    public boolean isTerminated()
    {
        return isTerminated.get();
    }

    public static class Config extends Flusher.Config<SyncFlusher, SyncFlusher.Config>
    {
        private float bufferOccupancyThreshold = 0.6f;

        public float getBufferOccupancyThreshold()
        {
            return bufferOccupancyThreshold;
        }

        public SyncFlusher.Config setBufferOccupancyThreshold(float bufferOccupancyThreshold)
        {
            this.bufferOccupancyThreshold = bufferOccupancyThreshold;
            return this;
        }

        @Override
        protected SyncFlusher.Config self() {
            return this;
        }
        @Override
        public SyncFlusher createInstance(Buffer buffer, Sender sender)
        {
            return new SyncFlusher(buffer, sender, this);
        }
    }
}
