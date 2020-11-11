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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.fluentd.ingester.Response;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.util.ExecutorServiceUtils;
import org.komamitsu.fluency.validation.annotation.Min;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class NetworkSender<T>
        extends FluentdSender
{
    private static final Logger LOG = LoggerFactory.getLogger(NetworkSender.class);
    private static final Charset CHARSET_FOR_ERRORLOG = Charset.forName("UTF-8");
    private final byte[] optionBuffer = new byte[256];
    private final ExecutorService executorService = ExecutorServiceUtils.newSingleThreadDaemonExecutor();

    private final Config config;
    private final FailureDetector failureDetector;
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    NetworkSender(FailureDetector failureDetector)
    {
        this(new Config(), failureDetector);
    }

    NetworkSender(Config config, FailureDetector failureDetector)
    {
        super(config);
        this.config = config;
        this.failureDetector = failureDetector;
    }

    @Override
    public boolean isAvailable()
    {
        return failureDetector == null || failureDetector.isAvailable();
    }

    abstract T getOrCreateSocketInternal()
            throws IOException;

    private synchronized T getOrCreateSocket()
            throws IOException
    {
        return getOrCreateSocketInternal();
    }

    abstract void sendBuffers(T socket, List<ByteBuffer> buffers)
            throws IOException;

    abstract void recvResponse(T socket, ByteBuffer buffer)
            throws IOException;

    private void propagateFailure(Throwable e)
    {
        if (failureDetector != null) {
            failureDetector.onFailure(e);
        }
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
            throws IOException
    {
        long totalDataSize = buffers.stream().mapToInt(ByteBuffer::remaining).sum();

        try {
//            LOG.trace("send(): sender.host={}, sender.port={}, totalDataSize={}",
//                    getHost(), getPort(), totalDataSize);
            final T socket = getOrCreateSocket();
            sendBuffers(socket, buffers);

            if (ackToken == null) {
                return;
            }

            // For ACK response mode
            final ByteBuffer byteBuffer = ByteBuffer.wrap(optionBuffer);

            Future<Void> future = executorService.submit(() -> {
                LOG.trace("recv(): sender.host={}, sender.port={}", getHost(), getPort());
                recvResponse(socket, byteBuffer);
                return null;
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

            Response response = objectMapper.readValue(optionBuffer, Response.class);
            byte[] unpackedToken = response.getAck();
            if (!Arrays.equals(ackToken, unpackedToken)) {
                throw new UnmatchedAckException("Ack tokens don't matched: expected=" + new String(ackToken, CHARSET_FOR_ERRORLOG) + ", got=" + new String(unpackedToken, CHARSET_FOR_ERRORLOG));
            }
        }
        catch (IOException e) {
            LOG.error("Failed to send {} bytes data", totalDataSize);
            closeSocket();
            propagateFailure(e);
            throw e;
        }
    }

    abstract void closeSocket()
            throws IOException;

    @Override
    public synchronized void close()
            throws IOException
    {
        try {
            // Wait to confirm unsent request is flushed
            try {
                TimeUnit.MILLISECONDS.sleep(config.getWaitBeforeCloseMilli());
            }
            catch (InterruptedException e) {
                LOG.warn("Interrupted", e);
                Thread.currentThread().interrupt();
            }

            closeSocket();
        }
        finally {
            try {
                if (failureDetector != null) {
                    failureDetector.close();
                }
            }
            finally {
                ExecutorServiceUtils.finishExecutorService(executorService);
            }
        }
    }

    public String getHost()
    {
        return config.getHost();
    }

    public int getPort()
    {
        return config.getPort();
    }

    public int getConnectionTimeoutMilli()
    {
        return config.getConnectionTimeoutMilli();
    }

    public int getReadTimeoutMilli()
    {
        return config.getReadTimeoutMilli();
    }

    public FailureDetector getFailureDetector()
    {
        return failureDetector;
    }

    @Override
    public String toString()
    {
        return "NetworkSender{" +
                "config=" + config +
                ", failureDetector=" + failureDetector +
                "} " + super.toString();
    }

    public static class UnmatchedAckException
            extends IOException
    {
        public UnmatchedAckException(String message)
        {
            super(message);
        }
    }

    public static class Config
            extends FluentdSender.Config
    {
        private String host = "127.0.0.1";
        private int port = 24224;
        @Min(10)
        private int connectionTimeoutMilli = 5000;
        @Min(10)
        private int readTimeoutMilli = 5000;
        @Min(0)
        private int waitBeforeCloseMilli = 1000;

        public String getHost()
        {
            return host;
        }

        public void setHost(String host)
        {
            this.host = host;
        }

        public int getPort()
        {
            return port;
        }

        public void setPort(int port)
        {
            this.port = port;
        }

        public int getConnectionTimeoutMilli()
        {
            return connectionTimeoutMilli;
        }

        public void setConnectionTimeoutMilli(int connectionTimeoutMilli)
        {
            this.connectionTimeoutMilli = connectionTimeoutMilli;
        }

        public int getReadTimeoutMilli()
        {
            return readTimeoutMilli;
        }

        public void setReadTimeoutMilli(int readTimeoutMilli)
        {
            this.readTimeoutMilli = readTimeoutMilli;
        }

        public int getWaitBeforeCloseMilli()
        {
            return waitBeforeCloseMilli;
        }

        public void setWaitBeforeCloseMilli(int waitBeforeCloseMilli)
        {
            this.waitBeforeCloseMilli = waitBeforeCloseMilli;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", connectionTimeoutMilli=" + connectionTimeoutMilli +
                    ", readTimeoutMilli=" + readTimeoutMilli +
                    ", waitBeforeCloseMilli=" + waitBeforeCloseMilli +
                    "} " + super.toString();
        }
    }
}
