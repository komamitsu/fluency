package org.komamitsu.fluency.sender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

public class TCPSender
    implements Sender
{
    private final AtomicReference<SocketChannel> channel = new AtomicReference<SocketChannel>();
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
    }

    private synchronized SocketChannel getOrOpenChannel()
            throws IOException
    {
        if (channel.get() == null) {
            channel.set(SocketChannel.open(new InetSocketAddress(host, port)));
        }
        return channel.get();
    }

    public void send(ByteBuffer data)
            throws IOException
    {
        getOrOpenChannel().write(data);
    }

    @Override
    public void close()
            throws IOException
    {
        SocketChannel socketChannel;
        if ((socketChannel = channel.getAndSet(null)) != null) {
            socketChannel.close();
        }
    }
}
