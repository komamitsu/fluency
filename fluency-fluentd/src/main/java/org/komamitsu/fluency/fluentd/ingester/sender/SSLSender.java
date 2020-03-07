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

import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SSLSender
        extends NetworkSender<SSLSocket>
{
    private final AtomicReference<SSLSocket> socket = new AtomicReference<>();
    private final SSLSocketBuilder socketBuilder;
    private final Config config;

    public SSLSender()
    {
        this(new Config());
    }

    public SSLSender(Config config)
    {
        this(config, null);
    }

    public SSLSender(FailureDetector failureDetector)
    {
        this(new Config(), failureDetector);
    }

    public SSLSender(Config config, FailureDetector failureDetector)
    {
        super(config, failureDetector);
        config.validateValues();
        socketBuilder = new SSLSocketBuilder(
                config.getHost(),
                config.getPort(),
                config.getConnectionTimeoutMilli(),
                config.getReadTimeoutMilli());
        this.config = config;
    }

    @Override
    protected SSLSocket getOrCreateSocketInternal()
            throws IOException
    {
        if (socket.get() == null) {
            socket.set(socketBuilder.build());
        }
        return socket.get();
    }

    @Override
    protected void sendBuffers(SSLSocket sslSocket, List<ByteBuffer> buffers)
            throws IOException
    {
        for (ByteBuffer buffer : buffers) {
            OutputStream outputStream = sslSocket.getOutputStream();
            if (buffer.hasArray()) {
                outputStream.write(buffer.array(), buffer.position(), buffer.remaining());
            }
            else {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                outputStream.write(bytes);
            }
        }
    }

    @Override
    protected void recvResponse(SSLSocket sslSocket, ByteBuffer buffer)
            throws IOException
    {
        InputStream inputStream = sslSocket.getInputStream();
        byte[] tempBuf = new byte[buffer.remaining()];
        int read = inputStream.read(tempBuf);
        buffer.put(tempBuf, 0, read);
    }

    @Override
    protected void closeSocket()
            throws IOException
    {
        SSLSocket existingSocket;
        if ((existingSocket = socket.getAndSet(null)) != null) {
            existingSocket.close();
        }
    }

    @Override
    public String toString()
    {
        return "SSLSender{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            extends NetworkSender.Config
            implements Validatable
    {
        void validateValues()
        {
            validate();
        }
    }
}
