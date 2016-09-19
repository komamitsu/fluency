package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.sender.MultiSender;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.sender.retry.ExponentialBackOffRetryStrategy;
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

public class Fluency
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Fluency.class);
    private final Buffer buffer;
    private final Flusher flusher;

    public static Fluency defaultFluency(String host, int port, Config config)
            throws IOException
    {
        return buildDefaultFluency(new TCPSender.Config().setHost(host).setPort(port), config);
    }

    private static Fluency buildDefaultFluency(Sender.Instantiator senderConfig, Config config)
    {
        PackedForwardBuffer.Config bufferConfig = new PackedForwardBuffer.Config();
        ExponentialBackOffRetryStrategy.Config retryStrategyConfig = new ExponentialBackOffRetryStrategy.Config();
        AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config();

        if (config != null) {
            if (config.getMaxBufferSize() != null) {
                bufferConfig.setMaxBufferSize(config.getMaxBufferSize());
            }

            if (config.getBufferChunkInitialSize() != null) {
                bufferConfig.setChunkInitialSize(config.getBufferChunkInitialSize());
            }

            if (config.getBufferChunkRetentionSize() != null) {
                bufferConfig.setChunkRetentionSize(config.getBufferChunkRetentionSize());
            }

            bufferConfig.setAckResponseMode(config.isAckResponseMode());

            if (config.getFileBackupDir() != null) {
                bufferConfig.setFileBackupDir(config.getFileBackupDir());
            }

            if (config.getFlushIntervalMillis() != null) {
                flusherConfig.setFlushIntervalMillis(config.getFlushIntervalMillis());
            }

            if (config.getWaitUntilBufferFlushed() != null) {
                flusherConfig.setWaitUntilBufferFlushed(config.getWaitUntilBufferFlushed());
            }

            if (config.getWaitUntilFlusherTerminated() != null) {
                flusherConfig.setWaitUntilTerminated(config.getWaitUntilFlusherTerminated());
            }

            if (config.getSenderMaxRetryCount() != null) {
                retryStrategyConfig.setMaxRetryCount(config.getSenderMaxRetryCount());
            }
        }

        RetryableSender retryableSender =
                new RetryableSender.Config(senderConfig)
                        .setRetryStrategyConfig(retryStrategyConfig)
                        .createInstance();

        return new Fluency.Builder(retryableSender)
                .setBufferConfig(bufferConfig)
                .setFlusherConfig(flusherConfig)
                .build();
    }

    public static Fluency defaultFluency(int port, Config config)
            throws IOException
    {
        return buildDefaultFluency(new TCPSender.Config().setPort(port), config);
    }

    public static Fluency defaultFluency(Config config)
            throws IOException
    {
        return buildDefaultFluency(new TCPSender.Config(), config);
    }

    public static Fluency defaultFluency(List<InetSocketAddress> servers, Config config)
            throws IOException
    {
        List<Sender.Instantiator> tcpSenderConfigs = new ArrayList<Sender.Instantiator>();
        for (InetSocketAddress server : servers) {
            tcpSenderConfigs.add(
                    new TCPSender.Config()
                            .setHost(server.getHostName())
                            .setPort(server.getPort())
                            .setHeartbeaterConfig(
                                    new TCPHeartbeater.Config()
                                            .setHost(server.getHostName())
                                            .setPort(server.getPort())));
        }
        return buildDefaultFluency(new MultiSender.Config(tcpSenderConfigs), config);
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
        return buildDefaultFluency(new TCPSender.Config(), null);
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
        try {
            buffer.append(tag, timestamp, data);
            flusher.onUpdate();
        }
        catch (BufferFullException e) {
            LOG.error("emit() failed due to buffer full. Flushing buffer. Please try again...");
            flusher.flush();
            throw e;
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

    public void clearBackupFiles()
    {
        buffer.clearBackupFiles();
    }

    public long getAllocatedBufferSize()
    {
        return buffer.getAllocatedSize();
    }

    public long getBufferedDataSize()
    {
        return buffer.getBufferedDataSize();
    }

    public boolean isTerminated()
    {
        return flusher.isTerminated();
    }

    public boolean waitUntilAllBufferFlushed(int maxWaitSeconds)
            throws InterruptedException
    {
        int intervalMilli = 500;
        for (int i = 0; i < maxWaitSeconds * (1000 / intervalMilli); i++) {
            long bufferedDataSize = getBufferedDataSize();
            LOG.info("Waiting for flushing all buffer: {}", bufferedDataSize);
            if (getBufferedDataSize() == 0) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(intervalMilli);
        }
        LOG.warn("Buffered data still remains: {}", getBufferedDataSize());
        return false;
    }

    public boolean waitUntilFlusherTerminated(int maxWaitSeconds)
            throws InterruptedException
    {
        int intervalMilli = 500;
        for (int i = 0; i < maxWaitSeconds * (1000 / intervalMilli); i++) {
            boolean terminated = isTerminated();
            LOG.info("Waiting until the flusher is terminated: {}", terminated);
            if (terminated) {
                return true;
            }
            TimeUnit.MILLISECONDS.sleep(intervalMilli);
        }
        LOG.warn("The flusher isn't terminated");
        return false;
    }

    public Buffer getBuffer()
    {
        return buffer;
    }

    public Flusher getFlusher()
    {
        return flusher;
    }

    @Override
    public String toString()
    {
        return "Fluency{" +
                "buffer=" + buffer +
                ", flusher=" + flusher +
                '}';
    }

    public static class Builder
    {
        private final Sender sender;
        private Buffer.Instantiator bufferConfig;
        private Flusher.Instantiator flusherConfig;

        public Builder(Sender sender)
        {
            this.sender = sender;
        }

        public Builder setBufferConfig(Buffer.Instantiator bufferConfig)
        {
            this.bufferConfig = bufferConfig;
            return this;
        }

        public Builder setFlusherConfig(Flusher.Instantiator flusherConfig)
        {
            this.flusherConfig = flusherConfig;
            return this;
        }

        public Fluency build()
        {
            Buffer.Instantiator bufferConfig = this.bufferConfig != null ? this.bufferConfig : new PackedForwardBuffer.Config();
            Buffer buffer = bufferConfig.createInstance();
            Flusher.Instantiator flusherConfig = this.flusherConfig != null ? this.flusherConfig : new AsyncFlusher.Config();
            Flusher flusher = flusherConfig.createInstance(buffer, sender);

            return new Fluency(buffer, flusher);
        }
    }

    public static class Config
    {
        private Long maxBufferSize;

        private Integer bufferChunkInitialSize;

        private Integer bufferChunkRetentionSize;

        private Integer flushIntervalMillis;

        private Integer senderMaxRetryCount;

        private boolean ackResponseMode;

        private String fileBackupDir;

        private Integer waitUntilBufferFlushed;

        private Integer waitUntilFlusherTerminated;

        public Long getMaxBufferSize()
        {
            return maxBufferSize;
        }

        public Config setMaxBufferSize(Long maxBufferSize)
        {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public Integer getBufferChunkInitialSize()
        {
            return bufferChunkInitialSize;
        }

        public Config setBufferChunkInitialSize(Integer bufferChunkInitialSize)
        {
            this.bufferChunkInitialSize = bufferChunkInitialSize;
            return this;
        }

        public Integer getBufferChunkRetentionSize()
        {
            return bufferChunkRetentionSize;
        }

        public Config setBufferChunkRetentionSize(Integer bufferChunkRetentionSize)
        {
            this.bufferChunkRetentionSize = bufferChunkRetentionSize;
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

        public boolean isAckResponseMode()
        {
            return ackResponseMode;
        }

        public Config setAckResponseMode(boolean ackResponseMode)
        {
            this.ackResponseMode = ackResponseMode;
            return this;
        }

        public String getFileBackupDir()
        {
            return fileBackupDir;
        }

        public Config setFileBackupDir(String fileBackupDir)
        {
            this.fileBackupDir = fileBackupDir;
            return this;
        }

        public Integer getWaitUntilBufferFlushed()
        {
            return waitUntilBufferFlushed;
        }

        public Config setWaitUntilBufferFlushed(Integer wait)
        {
            this.waitUntilBufferFlushed = wait;
            return this;
        }

        public Integer getWaitUntilFlusherTerminated()
        {
            return waitUntilFlusherTerminated;
        }

        public Config setWaitUntilFlusherTerminated(Integer wait)
        {
            this.waitUntilFlusherTerminated = wait;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "maxBufferSize=" + maxBufferSize +
                    ", bufferChunkInitialSize=" + bufferChunkInitialSize +
                    ", bufferChunkRetentionSize=" + bufferChunkRetentionSize +
                    ", flushIntervalMillis=" + flushIntervalMillis +
                    ", senderMaxRetryCount=" + senderMaxRetryCount +
                    ", ackResponseMode=" + ackResponseMode +
                    ", fileBackupDir='" + fileBackupDir + '\'' +
                    ", waitUntilBufferFlushed=" + waitUntilBufferFlushed +
                    ", waitUntilFlusherTerminated=" + waitUntilFlusherTerminated +
                    '}';
        }
    }
}
