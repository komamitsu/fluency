package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TCPSender
    extends NetworkSender
{
    private final AtomicReference<SocketChannel> channel = new AtomicReference<SocketChannel>();
    private final Config config;

    TCPSender(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
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

    protected synchronized void sendBuffers(List<ByteBuffer> buffers)
            throws IOException
    {
        getOrOpenChannel().write(buffers.toArray(new ByteBuffer[buffers.size()]));
    }

    protected void recvResponse(ByteBuffer buffer)
            throws IOException
    {
        getOrOpenChannel().read(buffer);
    }


    @Override
    protected void closeSocket()
            throws IOException
    {
        SocketChannel socketChannel;
        if ((socketChannel = channel.getAndSet(null)) != null) {
            socketChannel.close();
            channel.set(null);
        }
    }

    @Override
    public String toString()
    {
        return "TCPSender{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final NetworkSender.Config baseConfig = new NetworkSender.Config();

        public NetworkSender.Config getBaseConfig()
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
            return baseConfig.getHost();
        }

        public Config setHost(String host)
        {
            baseConfig.setHost(host);
            return this;
        }

        public int getPort()
        {
            return baseConfig.getPort();
        }

        public Config setPort(int port)
        {
            baseConfig.setPort(port);
            return this;
        }

        public int getConnectionTimeoutMilli()
        {
            return baseConfig.getConnectionTimeoutMilli();
        }

        public Config setConnectionTimeoutMilli(int connectionTimeoutMilli)
        {
            baseConfig.setConnectionTimeoutMilli(connectionTimeoutMilli);
            return this;
        }

        public int getReadTimeoutMilli()
        {
            return baseConfig.getReadTimeoutMilli();
        }

        public Config setReadTimeoutMilli(int readTimeoutMilli)
        {
            baseConfig.setReadTimeoutMilli(readTimeoutMilli);
            return this;
        }

        public Heartbeater.Instantiator getHeartbeaterConfig()
        {
            return baseConfig.getHeartbeaterConfig();
        }

        public Config setHeartbeaterConfig(Heartbeater.Instantiator heartbeaterConfig)
        {
            baseConfig.setHeartbeaterConfig(heartbeaterConfig);
            return this;
        }

        public FailureDetector.Config getFailureDetectorConfig()
        {
            return baseConfig.getFailureDetectorConfig();
        }

        public Config setFailureDetectorConfig(FailureDetector.Config failureDetectorConfig)
        {
            baseConfig.setFailureDetectorConfig(failureDetectorConfig);
            return this;
        }

        public FailureDetectStrategy.Instantiator getFailureDetectorStrategyConfig()
        {
            return baseConfig.getFailureDetectorStrategyConfig();
        }

        public Config setFailureDetectorStrategyConfig(FailureDetectStrategy.Instantiator failureDetectorStrategyConfig)
        {
            baseConfig.setFailureDetectorStrategyConfig(failureDetectorStrategyConfig);
            return this;
        }

        public int getWaitBeforeCloseMilli()
        {
            return baseConfig.getWaitBeforeCloseMilli();
        }

        public Config setWaitBeforeCloseMilli(int waitBeforeCloseMilli)
        {
            baseConfig.setWaitBeforeCloseMilli(waitBeforeCloseMilli);
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
                    '}';
        }
    }
}
