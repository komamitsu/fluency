package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SSLSender
    extends TCPSender
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSender.class);
    private static final String SSL_PROTOCOL = "TLSv1.2";
    private final Config config;
    private final AtomicReference<SSLSocket> socket = new AtomicReference<SSLSocket>();

    public SSLSender(Config config)
    {
        super(config.tcpSenderConfig);

        if (config.keystorePath == null || config.keystorePath.isEmpty()) {
            throw new IllegalArgumentException("Config.keystorePath: " + config.keystorePath);
        }

        this.config = config;
    }

    private KeyManager[] createKeyManagers()
    {
        KeyStore keyStore;
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(config.keystorePath);
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(fileInputStream, config.getStorePassword().toCharArray());
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to load KeyStore: keystorePath=%s", config.keystorePath), e);
        }
        catch (CertificateException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to load KeyStore: keystorePath=%s", config.keystorePath), e);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    String.format("Failed to get KeyStore: algorithm=%s",
                            KeyManagerFactory.getDefaultAlgorithm()));
        }
        catch (KeyStoreException e) {
            throw new IllegalStateException("Failed to get KeyStore", e);
        }
        finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                }
                catch (IOException e) {
                    LOG.warn("Failed to close certificate file", e);
                }
            }
        }

        try {
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, config.getKeyPassword().toCharArray());
            return keyManagerFactory.getKeyManagers();
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    String.format("Failed to get KeyManagerFactory: algorithm=%s",
                            KeyManagerFactory.getDefaultAlgorithm()));
        }
        catch (UnrecoverableKeyException e) {
            throw new IllegalStateException("Failed to initialize KeyManagerFactory", e);
        }
        catch (KeyStoreException e) {
            throw new IllegalStateException("Failed to initialize KeyManagerFactory", e);
        }
    }

    private SSLSocket getOrOpenSSLSocket()
            throws IOException
    {
        if (socket.get() == null) {
            try {
                KeyManager[] keyManagers = createKeyManagers();

                SSLContext sslContext = SSLContext.getInstance(SSL_PROTOCOL);
                sslContext.init(keyManagers, null, new SecureRandom());
                SSLSocketFactory socketFactory = sslContext.getSocketFactory();
                socket.set((SSLSocket) socketFactory.createSocket(config.getHost(), config.getPort()));
            }
            catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Failed to get SSLContext", e);
            }
            catch (KeyManagementException e) {
                throw new IllegalStateException("Failed to init SSLContext", e);
            }
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
        }
    }

    public static class Config
        implements Instantiator
    {
        private final Sender.Config baseConfig = new Sender.Config();
        private final TCPSender.Config tcpSenderConfig = new TCPSender.Config();
        private String keystorePath;
        private String keyPassword;
        private String storePassword;

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

        /*
         * SSL sender config
         */
        public String getKeystorePath()
        {
            return keystorePath;
        }

        public String getKeyPassword()
        {
            return keyPassword;
        }

        public Config setKeyPassword(String keyPassword)
        {
            this.keyPassword = keyPassword;
            return this;
        }

        public String getStorePassword()
        {
            return storePassword;
        }

        public Config setStorePassword(String storePassword)
        {
            this.storePassword = storePassword;
            return this;
        }

        public Config setKeystorePath(String keystorePath)
        {
            this.keystorePath = keystorePath;
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
                    ", keystorePath='" + keystorePath + '\'' +
                    ", keyPassword='" + keyPassword + '\'' +
                    ", storePassword='" + storePassword + '\'' +
                    '}';
        }
    }
}
