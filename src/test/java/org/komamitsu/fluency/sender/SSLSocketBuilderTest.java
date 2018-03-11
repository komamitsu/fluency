package org.komamitsu.fluency.sender;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SSLSocketBuilderTest
{
    private static final String KEYSTORE_PASSWORD = "storepassword";
    private static final String KEY_PASSWORD = "keypassword";
    private SSLServerSocket serverSocket;

    @Before
    public void setUp()
            throws IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException
    {
        String trustStorePath = SSLSocketBuilder.class.getClassLoader().getResource("truststore.jks").getFile();
        System.getProperties().setProperty("javax.net.ssl.trustStore", trustStorePath);

        String keyStorePath = SSLSocketBuilder.class.getClassLoader().getResource("keystore.jks").getFile();

        InputStream keystoreStream = null;
        try {
            KeyStore keystore = KeyStore.getInstance("JKS");
            keystoreStream = new FileInputStream(new File(keyStorePath));

            keystore.load(keystoreStream, KEYSTORE_PASSWORD.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            keyManagerFactory.init(keystore, KEY_PASSWORD.toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());

            serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket();
            serverSocket.setEnabledCipherSuites(serverSocket.getSupportedCipherSuites());
            serverSocket.bind(new InetSocketAddress(0));
        }
        finally {
            if (keystoreStream != null) {
                keystoreStream.close();
            }

        }
    }

    @After
    public void tearDown()
            throws IOException
    {
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    @Test
    public void testWithServer()
            throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        final AtomicInteger readLen = new AtomicInteger();
        final byte[] buf = new byte[256];

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Void> future = executorService.submit(new Callable<Void>()
        {
            @Override
            public Void call()
                    throws Exception
            {
                Socket clientSocket = serverSocket.accept();
                readLen.set(clientSocket.getInputStream().read(buf));
                return null;
            }
        });

        SSLSocket sslSocket = new SSLSocketBuilder()
                .setHost("localhost")
                .setPort(serverSocket.getLocalPort())
                .build();

        try {
            OutputStream outputStream = sslSocket.getOutputStream();
            outputStream.write("hello".getBytes("ASCII"));
            outputStream.flush();
        }
        finally {
            sslSocket.close();
        }

        future.get(10, TimeUnit.SECONDS);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(new String(buf, 0, readLen.get(), "ASCII"), "hello");
    }
}