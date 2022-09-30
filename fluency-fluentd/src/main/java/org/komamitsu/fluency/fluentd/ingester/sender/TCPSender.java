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

import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.validation.Validatable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TCPSender
        extends InetSocketSender<SocketChannel>
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSender.class);
    private final AtomicReference<SocketChannel> channel = new AtomicReference<>();
    private final Config config;

    public TCPSender()
    {
        this(new Config());
    }

    public TCPSender(Config config)
    {
        this(config, null);
    }

    public TCPSender(FailureDetector failureDetector)
    {
        this(new Config(), failureDetector);
    }

    public TCPSender(Config config, FailureDetector failureDetector)
    {
        super(config, failureDetector);
        config.validateValues();
        this.config = config;
    }

    @Override
    protected SocketChannel getOrCreateSocketInternal()
            throws IOException
    {
        if (channel.get() == null) {
            SocketChannel socketChannel = SocketChannel.open();
            try {
                socketChannel.socket().connect(new InetSocketAddress(config.getHost(), config.getPort()), config.getConnectionTimeoutMilli());
                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setSoTimeout(config.getReadTimeoutMilli());
            }
            catch (Throwable e) {
                // In case of java.net.UnknownHostException and so on, the internal socket can be leaked.
                // So the SocketChannel should be closed here to avoid a socket leak.
                socketChannel.close();
                throw e;
            }
            channel.set(socketChannel);
        }
        return channel.get();
    }

    @Override
    protected void sendBuffers(SocketChannel socketChannel, List<ByteBuffer> buffers)
            throws IOException
    {
        socketChannel.write(buffers.toArray(new ByteBuffer[0]));
    }

    @Override
    protected void recvResponse(SocketChannel socketChannel, ByteBuffer buffer)
            throws IOException
    {
        int read = socketChannel.read(buffer);
        if (read < 0) {
            throw new SocketException("The connection is disconnected by the peer");
        }
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
            extends InetSocketSender.Config
            implements Validatable
    {
        void validateValues()
        {
            validate();
        }
    }
}
