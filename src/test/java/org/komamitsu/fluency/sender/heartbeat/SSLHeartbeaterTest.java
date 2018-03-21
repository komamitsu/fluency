package org.komamitsu.fluency.sender.heartbeat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.sender.SSLTestServerSocketFactory;

import javax.net.ssl.SSLServerSocket;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSLHeartbeaterTest
{
    private SSLServerSocket sslServerSocket;
    private SSLHeartbeater heartbeater;

    @Before
    public void setUp()
            throws CertificateException, UnrecoverableKeyException, NoSuchAlgorithmException, IOException, KeyManagementException, KeyStoreException
    {
        sslServerSocket = new SSLTestServerSocketFactory().create();

        SSLHeartbeater.Config config = new SSLHeartbeater.Config().setPort(sslServerSocket.getLocalPort()).setIntervalMillis(500);
        heartbeater = config.createInstance();
    }

    @After
    public void tearDown()
            throws IOException
    {
        if (sslServerSocket != null && !sslServerSocket.isClosed()) {
            sslServerSocket.close();
        }
    }

    @Test
    public void testHeartbeaterUp()
            throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(2);
        Executors.newSingleThreadExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    sslServerSocket.accept();
                    latch.countDown();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        final AtomicInteger pongCounter  = new AtomicInteger();
        final AtomicInteger failureCounter  = new AtomicInteger();
        try {
            heartbeater.setCallback(new Heartbeater.Callback()
            {
                @Override
                public void onHeartbeat()
                {
                    pongCounter.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable cause)
                {
                    failureCounter.incrementAndGet();
                }
            });
            heartbeater.start();
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertTrue(0 < pongCounter.get() && pongCounter.get() < 3);
            assertEquals(0, failureCounter.get());
        }
        finally {
            if (heartbeater != null) {
                heartbeater.close();
            }
        }
    }

    @Test
    public void testHeartbeaterDown()
            throws IOException, InterruptedException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException
    {
        sslServerSocket.close();

        final AtomicInteger pongCounter  = new AtomicInteger();
        final AtomicInteger failureCounter  = new AtomicInteger();
        try {
            heartbeater.setCallback(new Heartbeater.Callback()
            {
                @Override
                public void onHeartbeat()
                {
                    pongCounter.incrementAndGet();
                }

                @Override
                public void onFailure(Throwable cause)
                {
                    failureCounter.incrementAndGet();
                }
            });
            heartbeater.start();
            TimeUnit.SECONDS.sleep(1);
            assertEquals(0, pongCounter.get());
            assertTrue(failureCounter.get() > 0);
        }
        finally {
            if (heartbeater != null) {
                heartbeater.close();
            }
        }
    }
}