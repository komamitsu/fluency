package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AsyncFlusher
        extends Flusher<AsyncFlusher.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(AsyncFlusher.class);
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Runnable task = new Runnable() {
            @Override
            public void run()
            {
                Event event = null;
                while (!executorService.isShutdown()) {
                    try {
                        event = eventQueue.poll(flusherConfig.getFlushIntervalMillis(), TimeUnit.MILLISECONDS);
                        boolean force = event != null;
                        buffer.flush(sender, force);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (IOException e) {
                        LOG.error("Failed to flush", e);
                    }

                    if (event == Event.CLOSE) {
                        break;
                    }
                }

                try {
                    if (event != Event.CLOSE) {
                        buffer.flush(sender, true);
                    }
                }
                catch (IOException e) {
                    LOG.error("Failed to flush", e);
                }
                closeBuffer();
            }
        };

    private enum Event
    {
        FORCE_FLUSH, CLOSE;
    }

    private AsyncFlusher(final Buffer buffer, final Sender sender, final Config flusherConfig)
    {
        super(buffer, sender, flusherConfig);
        executorService.execute(task);
    }

    @Override
    protected void flushInternal(boolean force)
            throws IOException
    {
        if (force) {
            try {
                eventQueue.put(Event.FORCE_FLUSH);
            }
            catch (InterruptedException e) {
                LOG.warn("Failed to force flushing buffer", e);
            }
        }
    }

    @Override
    protected void closeInternal()
            throws IOException
    {
        try {
            eventQueue.put(Event.CLOSE);
        }
        catch (InterruptedException e) {
            LOG.warn("Failed to close buffer", e);
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(flusherConfig.getWaitBeforeInterruptOnClose(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("1st awaitTermination was interrupted", e);
            Thread.currentThread().interrupt();
        }

        if (!executorService.isTerminated()) {
            executorService.shutdownNow();
        }
    }

    @Override
    public boolean isTerminated()
    {
        return executorService.isTerminated();
    }

    public static class Config extends Flusher.Config<AsyncFlusher, Flusher.Config>
    {
        @Override
        public AsyncFlusher createInstance(Buffer buffer, Sender sender)
        {
            return new AsyncFlusher(buffer, sender, this);
        }
    }
}
