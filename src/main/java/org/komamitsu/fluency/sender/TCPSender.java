package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
    extends Sender<TCPSender.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSender.class);
    private static final Charset CHARSET_FOR_ERRORLOG = Charset.forName("UTF-8");
    private final AtomicReference<SocketChannel> channel = new AtomicReference<SocketChannel>();
    private final byte[] optionBuffer = new byte[256];
    private final AckTokenSerDe ackTokenSerDe = new MessagePackAckTokenSerDe();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    @VisibleForTesting
    final FailureDetector failureDetector;

    public String getHost()
    {
        return config.getHost();
    }

    public int getPort()
    {
        return config.getPort();
    }

    public TCPSender(Config config)
    {
        super(config);
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
        return failureDetector.isAvailable();
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
        try {
            LOG.trace("send(): sender.host={}, sender.port={}", getHost(), getPort());
            getOrOpenChannel().write(dataList.toArray(new ByteBuffer[dataList.size()]));
        }
        catch (IOException e) {
            close();
            throw e;
        }
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
        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        buffers.addAll(dataList);
        if (ackToken != null) {
            buffers.add(ByteBuffer.wrap(ackTokenSerDe.pack(ackToken)));
        }
        try {
            sendBuffers(buffers);
        }
        catch (IOException e) {
            propagateFailure(e);
            throw e;
        }

        if (ackToken == null) {
            return;
        }

        // For ACK response mode
        final ByteBuffer byteBuffer = ByteBuffer.wrap(optionBuffer);

        try {
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

            byte[] unpackedToken = ackTokenSerDe.unpack(optionBuffer);
            if (!Arrays.equals(ackToken, unpackedToken)) {
                throw new UnmatchedAckException("Ack tokens don't matched: expected=" + new String(ackToken, CHARSET_FOR_ERRORLOG) + ", got=" + new String(unpackedToken, CHARSET_FOR_ERRORLOG));
            }
        }
        catch (IOException e) {
            close();
            propagateFailure(e);
            throw e;
        }
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        try {
            SocketChannel socketChannel;
            if ((socketChannel = channel.getAndSet(null)) != null) {
                socketChannel.close();
                channel.set(null);
            }
        }
        finally {
            if (failureDetector != null) {
                failureDetector.close();
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

    public static class Config extends Sender.Config<TCPSender, Config>
    {
        private String host = "127.0.0.1";
        private int port = 24224;
        private int connectionTimeoutMilli = 5000;
        private int readTimeoutMilli = 5000;
        private Heartbeater.Config heartbeaterConfig;   // Disabled by default
        private FailureDetector.Config failureDetectorConfig = new FailureDetector.Config();
        private FailureDetectStrategy.Config failureDetectorStrategyConfig = new PhiAccrualFailureDetectStrategy.Config();

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

        public Heartbeater.Config getHeartbeaterConfig()
        {
            return heartbeaterConfig;
        }

        public Config setHeartbeaterConfig(Heartbeater.Config heartbeaterConfig)
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

        public FailureDetectStrategy.Config getFailureDetectorStrategyConfig()
        {
            return failureDetectorStrategyConfig;
        }

        public Config setFailureDetectorStrategyConfig(FailureDetectStrategy.Config failureDetectorStrategyConfig)
        {
            this.failureDetectorStrategyConfig = failureDetectorStrategyConfig;
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
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", connectionTimeoutMilli=" + connectionTimeoutMilli +
                    ", readTimeoutMilli=" + readTimeoutMilli +
                    ", heartbeaterConfig=" + heartbeaterConfig +
                    ", failureDetectorConfig=" + failureDetectorConfig +
                    ", failureDetectorStrategyConfig=" + failureDetectorStrategyConfig +
                    "} " + super.toString();
        }
    }
}
