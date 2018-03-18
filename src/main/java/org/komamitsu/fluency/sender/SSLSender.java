package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;

import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SSLSender
    extends TCPSender
{
    private final AtomicReference<SSLSocket> socket = new AtomicReference<SSLSocket>();
    private final SSLSocketBuilder socketBuilder;
    private final Config config;

    public SSLSender(Config config)
    {
        super(config.tcpSenderConfig);
        socketBuilder = new SSLSocketBuilder(
                config.getHost(),
                config.getPort(),
                config.getConnectionTimeoutMilli(),
                config.getConnectionTimeoutMilli());
        this.config = config;
    }

    private SSLSocket getOrOpenSSLSocket()
            throws IOException
    {
        if (socket.get() == null) {
            socket.set(socketBuilder.build());
        }
        return socket.get();
    }

    @Override
    protected synchronized void sendBuffers(List<ByteBuffer> buffers)
            throws IOException
    {
        for (ByteBuffer buffer : buffers) {
            OutputStream outputStream = getOrOpenSSLSocket().getOutputStream();
            if (buffer.isDirect()) {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                outputStream.write(bytes);
            }
            else {
                outputStream.write(buffer.array());
            }
        }
    }

    @Override
    protected void recvResponse(ByteBuffer buffer)
            throws IOException
    {
        InputStream inputStream = getOrOpenSSLSocket().getInputStream();
        // TODO: a bit naive implementation
        int read = inputStream.read(optionBuffer);
        buffer.put(optionBuffer, 0, read);
    }

    @Override
    protected void closeSocket()
            throws IOException
    {
        if (socket.get() != null) {
            getOrOpenSSLSocket().close();
            socket.set(null);
        }
    }

    @Override
    public String toString()
    {
        return "SSLSender{" +
                "socketBuilder=" + socketBuilder +
                ", config=" + config +
                "} " + super.toString();
    }

    public static class Config
        implements Instantiator
    {
        private final Sender.Config baseConfig = new Sender.Config();
        private final TCPSender.Config tcpSenderConfig = new TCPSender.Config();

        /*
         * Base config
         */
        public SenderErrorHandler getSenderErrorHandler()
        {
            return baseConfig.getSenderErrorHandler();
        }

        public Config setSenderErrorHandler(SenderErrorHandler senderErrorHandler)
        {
            baseConfig.setSenderErrorHandler(senderErrorHandler);
            return this;
        }

        /*
         * TCP sender config
         */
        public String getHost()
        {
            return tcpSenderConfig.getHost();
        }

        public Config setHost(String host)
        {
            tcpSenderConfig.setHost(host);
            return this;
        }

        public int getPort()
        {
            return tcpSenderConfig.getPort();
        }

        public Config setPort(int port)
        {
            tcpSenderConfig.setPort(port);
            return this;
        }

        public int getConnectionTimeoutMilli()
        {
            return tcpSenderConfig.getConnectionTimeoutMilli();
        }

        public Config setConnectionTimeoutMilli(int connectionTimeoutMilli)
        {
            tcpSenderConfig.setConnectionTimeoutMilli(connectionTimeoutMilli);
            return this;
        }

        public int getReadTimeoutMilli()
        {
            return tcpSenderConfig.getReadTimeoutMilli();
        }

        public Config setReadTimeoutMilli(int readTimeoutMilli)
        {
            tcpSenderConfig.setReadTimeoutMilli(readTimeoutMilli);
            return this;
        }

        public Heartbeater.Instantiator getHeartbeaterConfig()
        {
            return tcpSenderConfig.getHeartbeaterConfig();
        }

        public Config setHeartbeaterConfig(Heartbeater.Instantiator heartbeaterConfig)
        {
            tcpSenderConfig.setHeartbeaterConfig(heartbeaterConfig);
            return this;
        }

        public FailureDetector.Config getFailureDetectorConfig()
        {
            return tcpSenderConfig.getFailureDetectorConfig();
        }

        public Config setFailureDetectorConfig(FailureDetector.Config failureDetectorConfig)
        {
            tcpSenderConfig.setFailureDetectorConfig(failureDetectorConfig);
            return this;
        }

        public FailureDetectStrategy.Instantiator getFailureDetectorStrategyConfig()
        {
            return tcpSenderConfig.getFailureDetectorStrategyConfig();
        }

        public Config setFailureDetectorStrategyConfig(FailureDetectStrategy.Instantiator failureDetectorStrategyConfig)
        {
            tcpSenderConfig.setFailureDetectorStrategyConfig(failureDetectorStrategyConfig);
            return this;
        }

        public int getWaitBeforeCloseMilli()
        {
            return tcpSenderConfig.getWaitBeforeCloseMilli();
        }

        public Config setWaitBeforeCloseMilli(int waitBeforeCloseMilli)
        {
            tcpSenderConfig.setWaitBeforeCloseMilli(waitBeforeCloseMilli);
            return this;
        }

        @Override
        public SSLSender createInstance()
        {
            return new SSLSender(this);
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", tcpSenderConfig=" + tcpSenderConfig +
                    '}';
        }
    }
}
