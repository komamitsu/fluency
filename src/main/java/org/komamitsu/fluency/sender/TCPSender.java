package org.komamitsu.fluency.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.format.Response;
import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.util.ExecutorServiceUtils;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class TCPSender
    extends Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSender.class);
    private static final Charset CHARSET_FOR_ERRORLOG = Charset.forName("UTF-8");
    private final AtomicReference<SocketChannel> channel = new AtomicReference<SocketChannel>();
    private final byte[] optionBuffer = new byte[256];
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Config config;
    private final FailureDetector failureDetector;
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    protected TCPSender(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
        FailureDetector failureDetector = null;
        if (config.getHeartbeaterConfig() != null) {
            try {
                failureDetector = new FailureDetector(
                        config.getFailureDetectorStrategyConfig(),
                        config.getHeartbeaterConfig(),
                        config.getFailureDetectorConfig());
            }
            catch (IOException e) {
                LOG.warn("Failed to instantiate FailureDetector. Disabling it", e);
            }
        }
        this.failureDetector = failureDetector;
    }

    @Override
    public boolean isAvailable()
    {
        return failureDetector == null || failureDetector.isAvailable();
    }

    private SocketChannel getOrOpenChannel()
            throws IOException
    {
        if (channel.get() == null) {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.socket().connect(new InetSocketAddress(config.getHost(), config.getPort()), config.getConnectionTimeoutMilli());
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSoTimeout(config.getReadTimeoutMilli());

            channel.set(socketChannel);
        }
        return channel.get();
    }

    private synchronized void sendBuffers(List<ByteBuffer> dataList)
            throws IOException
    {
        LOG.trace("send(): sender.host={}, sender.port={}", getHost(), getPort());
        getOrOpenChannel().write(dataList.toArray(new ByteBuffer[dataList.size()]));
    }

    private void propagateFailure(Throwable e)
    {
        if (failureDetector != null) {
            failureDetector.onFailure(e);
        }
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        try {
            sendBuffers(dataList);

            if (ackToken == null) {
                return;
            }

            // For ACK response mode
            final ByteBuffer byteBuffer = ByteBuffer.wrap(optionBuffer);

            Future<Void> future = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    getOrOpenChannel().read(byteBuffer);
                    return null;
                }
            });

            try {
                future.get(config.getReadTimeoutMilli(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new IOException("InterruptedException occurred", e);
            }
            catch (ExecutionException e) {
                throw new IOException("ExecutionException occurred", e);
            }
            catch (TimeoutException e) {
                throw new SocketTimeoutException("Socket read timeout");
            }

            Response response = objectMapper.readValue(optionBuffer, Response.class);
            byte[] unpackedToken = response.getAck();
            if (!Arrays.equals(ackToken, unpackedToken)) {
                throw new UnmatchedAckException("Ack tokens don't matched: expected=" + new String(ackToken, CHARSET_FOR_ERRORLOG) + ", got=" + new String(unpackedToken, CHARSET_FOR_ERRORLOG));
            }
        }
        catch (IOException e) {
            closeSocket();
            propagateFailure(e);
            throw e;
        }
    }

    private void closeSocket()
            throws IOException
    {
        SocketChannel socketChannel;
        if ((socketChannel = channel.getAndSet(null)) != null) {
            socketChannel.close();
            channel.set(null);
        }
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        try {
            // Wait to confirm unsent request is flushed
            try {
                TimeUnit.MILLISECONDS.sleep(config.getWaitBeforeCloseMilli());
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
                Thread.currentThread().interrupt();
            }

            closeSocket();
        }
        finally {
            try {
                if (failureDetector != null) {
                    failureDetector.close();
                }
            }
            finally {
                ExecutorServiceUtils.finishExecutorService(executorService);
            }
        }
    }

    public static class UnmatchedAckException
            extends IOException
    {
        public UnmatchedAckException(String message)
        {
            super(message);
        }
    }

    public String getHost()
    {
        return config.getHost();
    }

    public int getPort()
    {
        return config.getPort();
    }

    public int getConnectionTimeoutMilli()
    {
        return config.getConnectionTimeoutMilli();
    }

    public int getReadTimeoutMilli()
    {
        return config.getReadTimeoutMilli();
    }

    public FailureDetector getFailureDetector()
    {
        return failureDetector;
    }

    @Override
    public String toString()
    {
        return "TCPSender{" +
                "channel=" + channel +
                ", config=" + config +
                ", failureDetector=" + failureDetector +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final Sender.Config baseConfig = new Sender.Config();
        private String host = "127.0.0.1";
        private int port = 24224;
        private int connectionTimeoutMilli = 5000;
        private int readTimeoutMilli = 5000;
        private Heartbeater.Instantiator heartbeaterConfig;   // Disabled by default
        private FailureDetector.Config failureDetectorConfig = new FailureDetector.Config();
        private FailureDetectStrategy.Instantiator failureDetectorStrategyConfig = new PhiAccrualFailureDetectStrategy.Config();
        private int waitBeforeCloseMilli = 1000;

        public Sender.Config getBaseConfig()
        {
            return baseConfig;
        }

        public SenderErrorHandler getSenderErrorHandler()
        {
            return baseConfig.getSenderErrorHandler();
        }

        public Config setSenderErrorHandler(SenderErrorHandler senderErrorHandler)
        {
            baseConfig.setSenderErrorHandler(senderErrorHandler);
            return this;
        }

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

        public int getConnectionTimeoutMilli()
        {
            return connectionTimeoutMilli;
        }

        public Config setConnectionTimeoutMilli(int connectionTimeoutMilli)
        {
            this.connectionTimeoutMilli = connectionTimeoutMilli;
            return this;
        }

        public int getReadTimeoutMilli()
        {
            return readTimeoutMilli;
        }

        public Config setReadTimeoutMilli(int readTimeoutMilli)
        {
            this.readTimeoutMilli = readTimeoutMilli;
            return this;
        }

        public Heartbeater.Instantiator getHeartbeaterConfig()
        {
            return heartbeaterConfig;
        }

        public Config setHeartbeaterConfig(Heartbeater.Instantiator heartbeaterConfig)
        {
            this.heartbeaterConfig = heartbeaterConfig;
            return this;
        }

        public FailureDetector.Config getFailureDetectorConfig()
        {
            return failureDetectorConfig;
        }

        public Config setFailureDetectorConfig(FailureDetector.Config failureDetectorConfig)
        {
            this.failureDetectorConfig = failureDetectorConfig;
            return this;
        }

        public FailureDetectStrategy.Instantiator getFailureDetectorStrategyConfig()
        {
            return failureDetectorStrategyConfig;
        }

        public Config setFailureDetectorStrategyConfig(FailureDetectStrategy.Instantiator failureDetectorStrategyConfig)
        {
            this.failureDetectorStrategyConfig = failureDetectorStrategyConfig;
            return this;
        }

        public int getWaitBeforeCloseMilli()
        {
            return waitBeforeCloseMilli;
        }

        public Config setWaitBeforeCloseMilli(int waitBeforeCloseMilli)
        {
            this.waitBeforeCloseMilli = waitBeforeCloseMilli;
            return this;
        }

        @Override
        public TCPSender createInstance()
        {
            return new TCPSender(this);
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", connectionTimeoutMilli=" + connectionTimeoutMilli +
                    ", readTimeoutMilli=" + readTimeoutMilli +
                    ", heartbeaterConfig=" + heartbeaterConfig +
                    ", failureDetectorConfig=" + failureDetectorConfig +
                    ", failureDetectorStrategyConfig=" + failureDetectorStrategyConfig +
                    ", waitBeforeCloseMilli=" + waitBeforeCloseMilli +
                    '}';
        }
    }
}
