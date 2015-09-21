package org.komamitsu.fluency.sender.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class TCPHeartbeater extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPHeartbeater.class);

    protected TCPHeartbeater(final Config config)
            throws IOException
    {
        super(config);
    }

    @Override
    protected void ping()
    {
        try {
            SocketChannel.open(new InetSocketAddress(config.getHost(), config.getPort()));
            pong();
        }
        catch (Throwable e) {
            LOG.warn("Failed to connect to fluentd: config=" + config, e);
        }
    }

    public static class Factory
            implements Heartbeater.Factory<TCPHeartbeater>
    {
        @Override
        public TCPHeartbeater create(Config config)
                throws IOException
        {
            return new TCPHeartbeater(config);
        }
    }
}
