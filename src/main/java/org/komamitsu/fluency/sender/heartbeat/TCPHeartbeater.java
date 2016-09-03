package org.komamitsu.fluency.sender.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class TCPHeartbeater extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPHeartbeater.class);

    private TCPHeartbeater(final Config config)
            throws IOException
    {
        super(config);
    }

    @Override
    protected void invoke()
            throws IOException
    {
        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(config.getHost(), config.getPort()));
            pong();
        }
        finally {
            if (socketChannel != null) {
                socketChannel.close();
            }
        }
    }

    public static class Config extends Heartbeater.Config<Config>
    {
        @Override
        public TCPHeartbeater createInstance()
                throws IOException
        {
            return new TCPHeartbeater(this);
        }
    }

}

