package org.komamitsu.fluency.fluentd.ingester.sender.heartbeat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class UnixSocketHeartbeaterTest
{
    private Path socketPath;
    private ServerSocketChannel serverSocketChannel;

    @BeforeEach
    void setUp()
            throws IOException
    {
        socketPath = Paths.get(System.getProperty("java.io.tmpdir"),
                String.format("fluency-unixsocket-hb-test-%s", UUID.randomUUID()));
        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);

        serverSocketChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        serverSocketChannel.bind(socketAddress);
    }

    @Test
    void testHeartbeaterUp()
            throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(2);
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                serverSocketChannel.accept();
                latch.countDown();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });

        UnixSocketHeartbeater.Config config = new UnixSocketHeartbeater.Config();
        config.setPath(socketPath);
        config.setIntervalMillis(500);
        try (UnixSocketHeartbeater heartbeater = new UnixSocketHeartbeater(config)) {
            final AtomicInteger pongCounter = new AtomicInteger();
            final AtomicInteger failureCounter = new AtomicInteger();
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
    }

    @Test
    void testHeartbeaterDown()
            throws IOException, InterruptedException
    {
        serverSocketChannel.close();

        UnixSocketHeartbeater.Config config = new UnixSocketHeartbeater.Config();
        config.setPath(socketPath);
        config.setIntervalMillis(500);
        try (UnixSocketHeartbeater heartbeater = new UnixSocketHeartbeater(config)) {
            final AtomicInteger pongCounter = new AtomicInteger();
            final AtomicInteger failureCounter = new AtomicInteger();
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
    }
}
