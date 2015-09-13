package org.komamitsu.fluency.sender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class TCPSender
    implements Sender
{
    private final SocketChannel channel;
    private final String host;
    private final int port;

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    public TCPSender(String host, int port)
            throws IOException
    {
        this.port = port;
        this.host = host;
        this.channel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    public SocketChannel getChannel()
    {
        return channel;
    }

    public void send(ByteBuffer data)
            throws IOException
    {
        channel.write(data);
    }

    @Override
    public void close()
            throws IOException
    {
        channel.close();
    }
}
