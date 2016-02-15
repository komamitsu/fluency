package org.komamitsu.fluency.sender;

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

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        buffers.addAll(dataList);
        if (ackToken != null) {
            buffers.add(ByteBuffer.wrap(ackTokenSerDe.pack(ackToken)));
        }
        sendBuffers(buffers);

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
            channel.set(null);
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

        @Override
        public TCPSender createInstance()
        {
            return new TCPSender(this);
        }
    }
}
