package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.sender.MultiSender;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.komamitsu.fluency.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.sender.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.komamitsu.fluency.Constants.*;

public class Fluency
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Fluency.class);
    private final Buffer buffer;
    private final Flusher flusher;

    public static Fluency defaultFluency(String host, int port, Config config)
            throws IOException
    {
        return buildDefaultFluency(new TCPSender(host, port), config);
    }

    private static Fluency buildDefaultFluency(Sender sender, Config config)
    {
        Buffer.Config bufferConfig = new PackedForwardBuffer.Config();
        if (config != null && config.getMaxBufferSize() != null) {
            bufferConfig.setMaxBufferSize(config.getMaxBufferSize());
        }

        Flusher.Config flusherConfig = new AsyncFlusher.Config();
        if (config != null && config.getFlushIntervalMillis() != null) {
            flusherConfig.setFlushIntervalMillis(config.getFlushIntervalMillis());
        }

        RetryStrategy.Config retryStrategyConfig = new ExponentialBackOffRetryStrategy.Config();
        if (config != null && config.getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(config.getSenderMaxRetryCount());
        }
        RetryStrategy retryStrategy = retryStrategyConfig.createInstance();

        return new Fluency.Builder(new RetryableSender(sender, retryStrategy)).
                setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
    }

    public static Fluency defaultFluency(int port, Config config)
            throws IOException
    {
        return defaultFluency(DEFAULT_HOST, port, config);
    }

    public static Fluency defaultFluency(Config config)
            throws IOException
    {
        return defaultFluency(DEFAULT_HOST, DEFAULT_PORT, config);
    }

    public static Fluency defaultFluency(List<InetSocketAddress> servers, Config config)
            throws IOException
    {
        List<TCPSender> tcpSenders = new ArrayList<TCPSender>();
        for (InetSocketAddress server : servers) {
            tcpSenders.add(new TCPSender(server.getHostName(), server.getPort()));
        }
        return buildDefaultFluency(new MultiSender(tcpSenders), config);
    }

    public static Fluency defaultFluency(String host, int port)
            throws IOException
    {
        return defaultFluency(host, port, null);
    }

    public static Fluency defaultFluency(int port)
            throws IOException
    {
        return defaultFluency(port, null);
    }

    public static Fluency defaultFluency()
            throws IOException
    {
        return defaultFluency(DEFAULT_HOST, DEFAULT_PORT);
    }

    public static Fluency defaultFluency(List<InetSocketAddress> servers)
            throws IOException
    {
        return defaultFluency(servers, null);
    }

    private Fluency(Buffer buffer, Flusher flusher)
    {
        this.buffer = buffer;
        this.flusher = flusher;
    }

    public void emit(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        while (true) {
            try {
                buffer.append(tag, timestamp, data);
                flusher.onUpdate();
                break;
            }
            catch (Buffer.BufferFullException e) {
                LOG.warn("emit() failed due to buffer full", e);
                // TODO: Make it configurable
                try {
                    flusher.flush();
                    TimeUnit.MILLISECONDS.sleep(400);
                }
                catch (InterruptedException e1) {
                    LOG.warn("Interrupted during retrying", e1);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void emit(String tag, Map<String, Object> data)
            throws IOException
    {
        emit(tag, System.currentTimeMillis() / 1000, data);
    }

    @Override
    public void flush()
            throws IOException
    {
        flusher.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flusher.close();
    }

    public static class Builder
    {
        private final Sender sender;
        private Buffer.Config bufferConfig;
        private Flusher.Config flusherConfig;

        public Builder(Sender sender)
        {
            this.sender = sender;
        }

        public Builder setBufferConfig(Buffer.Config bufferConfig)
        {
            this.bufferConfig = bufferConfig;
            return this;
        }

        public Builder setFlusherConfig(Flusher.Config flusherConfig)
        {
            this.flusherConfig = flusherConfig;
            return this;
        }

        public Fluency build()
        {
            Buffer.Config bufferConfig = this.bufferConfig != null ? this.bufferConfig : new PackedForwardBuffer.Config();
            Buffer buffer = bufferConfig.createInstance();
            Flusher.Config flusherConfig = this.flusherConfig != null ? this.flusherConfig : new AsyncFlusher.Config();
            Flusher flusher = flusherConfig.createInstance(buffer, sender);

            return new Fluency(buffer, flusher);
        }
    }

    public static class Config
    {
        private Integer maxBufferSize;

        private Integer flushIntervalMillis;

        private Integer senderMaxRetryCount;

        public Integer getMaxBufferSize()
        {
            return maxBufferSize;
        }

        public Config setMaxBufferSize(Integer maxBufferSize)
        {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Integer getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public Config setFlushIntervalMillis(Integer flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
            return this;
        }

        public Integer getSenderMaxRetryCount()
        {
            return senderMaxRetryCount;
        }

        public Config setSenderMaxRetryCount(Integer senderMaxRetryCount)
        {
            this.senderMaxRetryCount = senderMaxRetryCount;
            return this;
        }
    }
}
