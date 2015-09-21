package org.komamitsu.fluency.sender.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;

public class UDPHeartbeater
        extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(UDPHeartbeater.class);
    private final SocketAddress socketAddress;

    protected UDPHeartbeater(final Config config)
            throws IOException
    {
        super(config);
        socketAddress = new InetSocketAddress(config.getHost(), config.getPort());
    }

    @Override
    protected void ping()
    {
        try {
            DatagramChannel datagramChannel = DatagramChannel.open();
            ByteBuffer byteBuffer = ByteBuffer.allocate(0);
            datagramChannel.send(byteBuffer, socketAddress);
            datagramChannel.receive(byteBuffer);
            datagramChannel.close();
            pong();
        }
        catch (Throwable e) {
            LOG.warn("Failed to connect to fluentd: config=" + config, e);
        }
    }

    public static class Factory
            implements Heartbeater.Factory<UDPHeartbeater>
    {
        @Override
        public UDPHeartbeater create(Config config)
                throws IOException
        {
            return new UDPHeartbeater(config);
        }
    }
}
