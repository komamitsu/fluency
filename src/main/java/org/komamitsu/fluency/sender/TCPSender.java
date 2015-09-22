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

    public TCPSender(int port)
            throws IOException
    {
        this("127.0.0.1", port);
    }

    public TCPSender(String host)
            throws IOException
    {
        this(host, 24224);
    }

    public TCPSender()
            throws IOException
    {
        this("127.0.0.1", 24224);
    }

    private SocketChannel getOrOpenChannel()
            throws IOException
    {
        if (channel.get() == null) {
            SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
            socketChannel.socket().setTcpNoDelay(true);
            channel.set(socketChannel);
        }
        return channel.get();
    }

    public synchronized void send(ByteBuffer data)
            throws IOException
    {
        try {
            getOrOpenChannel().write(data);
        }
        catch (IOException e) {
            channel.set(null);
            throw e;
        }
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        SocketChannel socketChannel;
        if ((socketChannel = channel.getAndSet(null)) != null) {
            socketChannel.close();
        }
    }
}
