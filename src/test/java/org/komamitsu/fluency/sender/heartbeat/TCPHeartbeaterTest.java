package org.komamitsu.fluency.sender.heartbeat;

import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TCPHeartbeaterTest
{
    @Test
    public void testTCPHeartbeaterUp()
            throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(2);
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(null);
        Executors.newSingleThreadExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    serverSocketChannel.accept();
                    latch.countDown();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        TCPHeartbeater.Config config = new TCPHeartbeater.Config().setPort(serverSocketChannel.socket().getLocalPort()).setIntervalMillis(500);
        TCPHeartbeater heartbeater = null;
        try {
            final AtomicInteger pongCounter  = new AtomicInteger();
            final AtomicInteger failureCounter  = new AtomicInteger();
            heartbeater = config.createInstance();
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
            assertTrue(latch.await(5, TimeUnit.SECONDS));
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
    public void testTCPHeartbeaterDown()
            throws IOException, InterruptedException
    {
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(null);
        int localPort = serverSocketChannel.socket().getLocalPort();
        serverSocketChannel.close();

        TCPHeartbeater.Config config = new TCPHeartbeater.Config().setPort(localPort).setIntervalMillis(500);
        TCPHeartbeater heartbeater = null;
        try {
            final AtomicInteger pongCounter  = new AtomicInteger();
            final AtomicInteger failureCounter  = new AtomicInteger();
            heartbeater = config.createInstance();
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