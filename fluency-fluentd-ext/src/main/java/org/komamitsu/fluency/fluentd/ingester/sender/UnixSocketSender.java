/*
 * Copyright 2022 Mitsunori Komatsu (komamitsu)
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
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class UnixSocketSender
        extends NetworkSender<SocketChannel>
{
    private static final Logger LOG = LoggerFactory.getLogger(UnixSocketSender.class);
    private final AtomicReference<SocketChannel> channel = new AtomicReference<>();
    private final Config config;

    public UnixSocketSender()
    {
        this(new Config());
    }

    public UnixSocketSender(Config config)
    {
        this(config, null);
    }

    public UnixSocketSender(FailureDetector failureDetector)
    {
        this(new Config(), failureDetector);
    }

    public UnixSocketSender(Config config, FailureDetector failureDetector)
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
            UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(config.getPath());
            SocketChannel socketChannel = SocketChannel.open(socketAddress);
            try {
//                socketChannel.connect(UnixDomainSocketAddress.of(config.getPath()));
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
        return "UnixSocketSender{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            extends NetworkSender.Config
            implements Validatable
    {
        private Path path;

        void validateValues()
        {
            validate();
        }

        public Path getPath()
        {
            return path;
        }

        public void setPath(Path path)
        {
            this.path = path;
        }
    }
}
