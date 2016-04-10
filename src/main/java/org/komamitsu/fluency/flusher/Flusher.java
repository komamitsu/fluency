package org.komamitsu.fluency.flusher;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.rmi.server.ExportException;
import java.util.concurrent.TimeUnit;

public abstract class Flusher<C extends Flusher.Config>
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
    protected final Buffer buffer;
    protected final Sender sender;
    protected final C flusherConfig;

    public Flusher(Buffer buffer, Sender sender, C flusherConfig)
    {
        this.buffer = buffer;
        this.sender = sender;
        this.flusherConfig = flusherConfig;
    }

    public Buffer getBuffer()
    {
        return buffer;
    }

    protected abstract void flushInternal(boolean force)
            throws IOException;

    protected abstract void closeInternal()
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
        if (flusherConfig.getWaitUntilBufferFlushOnClose() != null) {
            int waitMilli = 500;
            int maxCount = flusherConfig.getWaitUntilBufferFlushOnClose() * (1000 / waitMilli);
            try {
                for (int i = 0; i < maxCount; i++) {
                    LOG.debug("Wating until buffers are all flushed: bufferedDataSize={}", buffer.getBufferedDataSize());
                    flushInternal(false);
                    if (buffer.getBufferedDataSize() == 0) {
                        break;
                    }
                    TimeUnit.MILLISECONDS.sleep(waitMilli);
                }
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting buffer flushes all data", e);
                Thread.currentThread().interrupt();
            }
            catch (Exception e) {
                LOG.warn("Failed to wait that buffer flushes all data", e);
            }
        }

        try {
            closeInternal();
        }
        finally {
            try {
                sender.close();
            }
            catch (Exception e) {
                LOG.warn("Failed to close sender", e);
            }
            finally {
                if (flusherConfig.getWaitUntilTerminatedOnClose() != null) {
                    int waitMilli = 500;
                    int maxCount = flusherConfig.getWaitUntilTerminatedOnClose() * (1000 / waitMilli);
                    try {
                        for (int i = 0; i < maxCount; i++) {
                            if (isTerminated()) {
                                break;
                            }
                            TimeUnit.MILLISECONDS.sleep(waitMilli);
                        }
                    }
                    catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting flusher gets terminated", e);
                        Thread.currentThread().interrupt();
                    }
                    catch (Exception e) {
                        LOG.warn("Failed to wait that flusher gets terminated", e);
                    }
                }
            }
        }
    }

    public abstract boolean isTerminated();

    protected void closeBuffer()
    {
        LOG.trace("closeBuffer(): closing buffer");
        buffer.close();
    }

    public abstract static class Config<T extends Flusher, C extends Config>
    {
        private int flushIntervalMillis = 600;

        private Integer waitUntilBufferFlushOnClose = 10;

        private Integer waitUntilTerminatedOnClose = 10;

        private Integer waitBeforeInterruptOnClose = 10;

        public int getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public C setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return (C)this;
        }

        public Integer getWaitUntilBufferFlushOnClose()
        {
            return waitUntilBufferFlushOnClose;
        }

        public C setWaitUntilBufferFlushOnClose(Integer waitUntilBufferFlushOnClose)
        {
            this.waitUntilBufferFlushOnClose = waitUntilBufferFlushOnClose;
            return (C)this;
        }

        public Integer getWaitUntilTerminatedOnClose()
        {
            return waitUntilTerminatedOnClose;
        }

        public C setWaitUntilTerminatedOnClose(Integer waitUntilTerminatedOnClose)
        {
            this.waitUntilTerminatedOnClose = waitUntilTerminatedOnClose;
            return (C)this;
        }

        public Integer getWaitBeforeInterruptOnClose()
        {
            return waitBeforeInterruptOnClose;
        }

        public C setWaitBeforeInterruptOnClose(Integer waitBeforeInterruptOnClose)
        {
            this.waitBeforeInterruptOnClose = waitBeforeInterruptOnClose;
            return (C) this;
        }

        public abstract T createInstance(Buffer buffer, Sender sender);
    }
}
