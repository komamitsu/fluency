package org.komamitsu.fluency.sender.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class TCPHeartbeater
        extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPHeartbeater.class);
    private final Config config;

    protected TCPHeartbeater(final Config config)
            throws IOException
    {
        super(config.getBaseConfig());
        this.config = config;
    }

    @Override
    protected void invoke()
            throws IOException
    {
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(config.getHost(), config.getPort()));
            LOG.trace("TCPHeartbeat: remotePort={}, localPort={}",
                    socketChannel.socket().getPort(), socketChannel.socket().getLocalPort());
            pong();
        }
        finally {
            if (socketChannel != null) {
                socketChannel.close();
            }
        }
    }

    @Override
    public String toString()
    {
        return "TCPHeartbeater{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final Heartbeater.Config baseConfig = new Heartbeater.Config();

        public Heartbeater.Config getBaseConfig()
        {
            return baseConfig;
        }

        public String getHost()
        {
            return baseConfig.getHost();
        }

        public int getPort()
        {
            return baseConfig.getPort();
        }

        public Config setIntervalMillis(int intervalMillis)
        {
            baseConfig.setIntervalMillis(intervalMillis);
            return this;
        }

        public int getIntervalMillis()
        {
            return baseConfig.getIntervalMillis();
        }

        public Config setHost(String host)
        {
            baseConfig.setHost(host);
            return this;
        }

        public Config setPort(int port)
        {
            baseConfig.setPort(port);
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    '}';
        }

        @Override
        public TCPHeartbeater createInstance()
                throws IOException
        {
            return new TCPHeartbeater(this);
        }
    }

}

