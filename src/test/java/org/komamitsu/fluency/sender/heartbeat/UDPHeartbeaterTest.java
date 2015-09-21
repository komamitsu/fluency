package org.komamitsu.fluency.sender.heartbeat;

import org.junit.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class UDPHeartbeaterTest
{
    @Test
    public void testUDPHeartbeater()
            throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(2);
        final DatagramChannel datagramChannel = DatagramChannel.open();
        datagramChannel.socket().bind(null);
        Executors.newSingleThreadExecutor().execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
                    SocketAddress socketAddress = datagramChannel.receive(byteBuffer);
                    datagramChannel.send(ByteBuffer.allocate(0), socketAddress);
                    latch.countDown();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Heartbeater.Config config = new Heartbeater.Config().setPort(datagramChannel.socket().getLocalPort()).setIntervalMillis(500);
        UDPHeartbeater heartbeater = null;
        try {
            final AtomicInteger pongCounter  = new AtomicInteger();
            heartbeater = new UDPHeartbeater(config);
            heartbeater.setCallback(new Heartbeater.Callback() {
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