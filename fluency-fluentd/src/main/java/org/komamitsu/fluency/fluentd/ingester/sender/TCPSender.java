/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.fluentd.ingester.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TCPSender
    extends NetworkSender<SocketChannel>
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSender.class);
    private final AtomicReference<SocketChannel> channel = new AtomicReference<SocketChannel>();
    private final Config config;

    public TCPSender(Config config)
    {
        super(config);
        this.config = config;
    }

    @Override
    protected SocketChannel getOrCreateSocketInternal()
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

    @Override
    protected void sendBuffers(SocketChannel socketChannel, List<ByteBuffer> buffers)
            throws IOException
    {
        socketChannel.write(buffers.toArray(new ByteBuffer[buffers.size()]));
    }

    @Override
    protected void recvResponse(SocketChannel socketChannel, ByteBuffer buffer)
            throws IOException
    {
        socketChannel.read(buffer);
    }

    @Override
    protected void closeSocket()
            throws IOException
    {
        SocketChannel existingSocketChannel;
        if ((existingSocketChannel = channel.getAndSet(null)) != null) {
            existingSocketChannel.close();
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
        extends NetworkSender.Config
    {
    }
}
