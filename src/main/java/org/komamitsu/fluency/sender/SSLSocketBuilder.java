package org.komamitsu.fluency.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class SSLSocketBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(SSLSocketBuilder.class);
    private static final String SSL_PROTOCOL = "TLSv1.2";
    private String host;
    private Integer port;
    private String keystorePath;
    private String keyPassword;
    private String storePassword;

    public SSLSocketBuilder setHost(String host)
    {
        this.host = host;
        return this;
    }

    public SSLSocketBuilder setPort(int port)
    {
        this.port = port;
        return this;
    }

    public SSLSocketBuilder setKeystorePath(String keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public SSLSocketBuilder setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public SSLSocketBuilder setStorePassword(String storePassword)
    {
        this.storePassword = storePassword;
        return this;
    }

    private KeyManager[] createKeyManagers()
    {
        KeyStore keyStore;
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(keystorePath);
            keyStore = KeyStore.getInstance("JKS");
            keyStore.load(fileInputStream, storePassword.toCharArray());
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to load KeyStore: keystorePath=%s", keystorePath), e);
        }
        catch (CertificateException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to load KeyStore: keystorePath=%s", keystorePath), e);
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
            keyManagerFactory.init(keyStore, keyPassword.toCharArray());
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

    public SSLSocket build()
            throws IOException
    {
        if (port == null) {
            throw new IllegalArgumentException("port is null");
        }
        if (keystorePath == null) {
            throw new IllegalArgumentException("keystorePath is null");
        }
        if (keyPassword == null) {
            throw new IllegalArgumentException("keyPassword is null");
        }
        if (storePassword == null) {
            throw new IllegalArgumentException("storePassword is null");
        }

        try {
            KeyManager[] keyManagers = createKeyManagers();

            SSLContext sslContext = SSLContext.getInstance(SSL_PROTOCOL);
            sslContext.init(keyManagers, null, new SecureRandom());
            javax.net.ssl.SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            return (SSLSocket) socketFactory.createSocket(host == null ? "127.0.0.1" : host, port);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to get SSLContext", e);
        }
        catch (KeyManagementException e) {
            throw new IllegalStateException("Failed to init SSLContext", e);
        }
    }
}
