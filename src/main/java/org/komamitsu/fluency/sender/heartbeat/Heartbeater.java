package org.komamitsu.fluency.sender.heartbeat;

import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Heartbeater implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(AsyncFlusher.class);
    private final Config config;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<Callback> callback = new AtomicReference<Callback>();

    protected Heartbeater(Config config)
    {
        this.config = config;
        executorService = Executors.newScheduledThreadPool(1);
    }

    public void start()
    {
        executorService.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                ping();
            }
        }, config.getIntervalMillis(), config.getIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    public String getHost()
    {
        return config.getHost();
    }

    public int getPort()
    {
        return config.getPort();
    }

    protected abstract void invoke()
            throws IOException;

    protected void ping()
    {
        try {
            invoke();
        }
        catch (Throwable e) {
            LOG.warn("ping(): failed, config=" + config);
            Callback callback = this.callback.get();
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }

    protected void pong()
    {
        Callback callback = this.callback.get();
        if (callback != null) {
            callback.onHeartbeat();
        }
    }

    public void setCallback(Callback callback)
    {
        this.callback.set(callback);
    }

    @Override
    public void close()
            throws IOException
    {
        executorService.shutdown();
        try {
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("1st awaitTermination was interrupted", e);
            Thread.currentThread().interrupt();
        }
        if (!executorService.isTerminated()) {
            executorService.shutdownNow();
        }
    }

    public interface Callback
    {
        void onHeartbeat();

        void onFailure(Throwable cause);
    }

    public static class Config
    {
        private String host = "127.0.0.1";
        private int port = 24224;
        private int intervalMillis = 1000;

        public String getHost()
        {
            return host;
        }

        public Config setHost(String host)
        {
            this.host = host;
            return this;
        }

        public int getPort()
        {
            return port;
        }

        public Config setPort(int port)
        {
            this.port = port;
            return this;
        }

        public int getIntervalMillis()
        {
            return intervalMillis;
        }

        public Config setIntervalMillis(int intervalMillis)
        {
            this.intervalMillis = intervalMillis;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", intervalMillis=" + intervalMillis +
                    '}';
        }
    }

    public interface Instantiator
    {
        Heartbeater createInstance()
                throws IOException;
    }
}
