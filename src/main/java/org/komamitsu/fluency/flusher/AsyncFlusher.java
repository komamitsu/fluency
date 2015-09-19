package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncFlusher
        extends Flusher
{
    private static final Logger LOG = LoggerFactory.getLogger(AsyncFlusher.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Runnable task = new Runnable() {
            @Override
            public void run()
            {
                while (!executorService.isShutdown()) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(flusherConfig.getFlushIntervalMillis());
                        buffer.flush(sender);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (IOException e) {
                        LOG.error("Failed to flush", e);
                    }
                }
            }
        };

    public AsyncFlusher(final Buffer buffer, final Sender sender, final FlusherConfig flusherConfig)
    {
        super(buffer, sender, flusherConfig);
        executorService.execute(task);
    }

    public AsyncFlusher(Buffer buffer, Sender sender)
    {
        this(buffer, sender, new FlusherConfig.Builder().build());
    }

    @Override
    protected void flushInternal(boolean force)
            throws IOException
    {
        if (force) {
            executorService.execute(task);
        }
    }

    @Override
    protected void closeInternal()
            throws IOException
    {
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("1st awaitTermination was interrupted", e);
            executorService.shutdownNow();
            try {
                executorService.awaitTermination(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e1) {
                LOG.warn("2nd awaitTermination was interrupted", e);
                e1.printStackTrace();
            }
        }
    }
}
