package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.sender.heartbeat.UDPHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class MultiSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSenderTest.class);

    @Test
    public void testConstructor()
            throws IOException
    {
        MultiSender multiSender = new MultiSender(Arrays.asList(new TCPSender(24225), new TCPSender("0.0.0.0", 24226)));
        assertEquals(2, multiSender.sendersAndFailureDetectors.size());

        assertEquals("127.0.0.1", multiSender.sendersAndFailureDetectors.get(0).getFirst().getHost());
        assertEquals(24225, multiSender.sendersAndFailureDetectors.get(0).getFirst().getPort());
        assertEquals("127.0.0.1", multiSender.sendersAndFailureDetectors.get(0).getSecond().getHeartbeater().getHost());
        assertEquals(24225, multiSender.sendersAndFailureDetectors.get(0).getSecond().getHeartbeater().getPort());

        assertEquals("0.0.0.0", multiSender.sendersAndFailureDetectors.get(1).getFirst().getHost());
        assertEquals(24226, multiSender.sendersAndFailureDetectors.get(1).getFirst().getPort());
        assertEquals("0.0.0.0", multiSender.sendersAndFailureDetectors.get(1).getSecond().getHeartbeater().getHost());
        assertEquals(24226, multiSender.sendersAndFailureDetectors.get(1).getSecond().getHeartbeater().getPort());
    }

    @Test
    public void testSend()
            throws IOException, InterruptedException
    {
        final MockMultiTCPServerWithMetrics server0 = new MockMultiTCPServerWithMetrics();
        server0.start();
        final MockMultiTCPServerWithMetrics server1 = new MockMultiTCPServerWithMetrics();
        server1.start();

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);

        final MultiSender sender = new MultiSender(Arrays.asList(new TCPSender(server0.getLocalPort()), new TCPSender(server1.getLocalPort())), new UDPHeartbeater.Config());
        final ExecutorService senderExecutorService = Executors.newCachedThreadPool();
        final AtomicBoolean shouldFailOver = new AtomicBoolean(true);
        for (int i = 0; i < concurency; i++) {
            senderExecutorService.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        byte[] bytes = "0123456789".getBytes();

                        for (int j = 0; j < reqNum; j++) {
                            if (j == reqNum / 4) {
                                if (shouldFailOver.getAndSet(false)) {
                                    LOG.info("Failing over...");
                                    server0.stop();
                                }
                            }
                            sender.send(ByteBuffer.wrap(bytes));
                        }
                        latch.countDown();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        latch.await(6, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        sender.close();
        TimeUnit.MILLISECONDS.sleep(500);

        server1.stop();

        int connectCount = 0;
        int closeCount = 0;
        long recvCount = 0;
        long recvLen = 0;
        for (MockMultiTCPServerWithMetrics server : Arrays.asList(server0, server1)) {
            for (Tuple<MockTCPServerWithMetrics.Type, Integer> event : server.getEvents()) {
                switch (event.getFirst()) {
                    case CONNECT:
                        connectCount++;
                        break;
                    case CLOSE:
                        closeCount++;
                        break;
                    case RECEIVE:
                        recvCount++;
                        recvLen += event.getSecond();
                        break;
                }
            }
        }
        LOG.debug("recvCount={}", recvCount);
        LOG.debug("recvLen={}", recvLen);

        assertEquals(2, connectCount);
        assertTrue(((concurency - 1) * reqNum) * 10 <= recvLen && recvLen <= (concurency * reqNum) * 10);
        assertEquals(2, closeCount);
 }
}