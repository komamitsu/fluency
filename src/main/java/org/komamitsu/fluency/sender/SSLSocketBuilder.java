package org.komamitsu.fluency.sender;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class SSLSocketBuilder
{
    private static final String SSL_PROTOCOL = "TLSv1.2";
    private String host;
    private Integer port;

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

    public SSLSocket build()
            throws IOException
    {
        if (port == null) {
            throw new IllegalArgumentException("port is null");
        }

        try {
            SSLContext sslContext = SSLContext.getInstance(SSL_PROTOCOL);
            sslContext.init(null, null, new SecureRandom());
            javax.net.ssl.SSLSocketFactory socketFactory = sslContext.getSocketFactory();
            return (SSLSocket) socketFactory.createSocket(host == null ? "localhost" : host, port);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to get SSLContext", e);
        }
        catch (KeyManagementException e) {
            throw new IllegalStateException("Failed to init SSLContext", e);
        }
    }
}
