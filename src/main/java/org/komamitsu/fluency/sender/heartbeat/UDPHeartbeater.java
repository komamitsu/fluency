package org.komamitsu.fluency.sender.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class UDPHeartbeater
        extends Heartbeater
{
    private static final Logger LOG = LoggerFactory.getLogger(UDPHeartbeater.class);
    private final SocketAddress socketAddress;

    private UDPHeartbeater(final Config config)
            throws IOException
    {
        super(config);
        socketAddress = new InetSocketAddress(config.getHost(), config.getPort());
    }

    @Override
    protected void invoke()
            throws IOException
    {
        DatagramChannel datagramChannel = null;
        try {
            datagramChannel = DatagramChannel.open();
            ByteBuffer byteBuffer = ByteBuffer.allocate(0);
            datagramChannel.send(byteBuffer, socketAddress);
            datagramChannel.receive(byteBuffer);
            pong();
        }
        finally {
            if (datagramChannel != null) {
                datagramChannel.close();
            }
        }
    }

    public static class Config extends Heartbeater.Config<Config>
    {
        @Override
        public UDPHeartbeater createInstance()
                throws IOException
        {
            return new UDPHeartbeater(this);
        }
    }
}
