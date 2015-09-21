package org.komamitsu.fluency.sender.heartbeat;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TCPHeartbeaterTest
{
    @Test
    public void testTCPHeartbeater()
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

        Heartbeater.Config config = new Heartbeater.Config().setPort(serverSocketChannel.socket().getLocalPort()).setIntervalMillis(500);
        TCPHeartbeater heartbeater = null;
        try {
            final AtomicInteger pongCounter  = new AtomicInteger();
            heartbeater = new TCPHeartbeater(config);
            heartbeater.setCallback(new Heartbeater.Callback()
            {
                @Override
                public void onHeartbeat()
                {
                    pongCounter.incrementAndGet();
                    latch.countDown();
                }
            });
            latch.await(5, TimeUnit.SECONDS);
            assertEquals(0, latch.getCount());
            assertTrue(0 < pongCounter.get() && pongCounter.get() < 3);
        }
        finally {
            if (heartbeater != null) {
                heartbeater.close();
            }
        }
    }
}