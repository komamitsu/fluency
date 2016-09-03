package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.sender.heartbeat.UDPHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
        MultiSender multiSender = null;
        try {
            multiSender = new MultiSender.Config(
                    Arrays.<Sender.Config>asList(
                            new TCPSender.Config()
                                    .setPort(24225)
                                    .setHeartbeaterConfig(
                                            new TCPHeartbeater.Config().
                                                    setPort(24225)),
                            new TCPSender.Config()
                                    .setHost("0.0.0.0")
                                    .setPort(24226)
                                    .setHeartbeaterConfig(
                                            new TCPHeartbeater.Config()
                                                    .setHost("0.0.0.0")
                                                    .setPort(24226))
                                    )).createInstance();

            assertEquals(2, multiSender.senders.size());

            TCPSender tcpSender = (TCPSender) multiSender.senders.get(0);
            assertEquals("127.0.0.1", tcpSender.getHost());
            assertEquals(24225, tcpSender.getPort());
            assertEquals("127.0.0.1", tcpSender.failureDetector.getHeartbeater().getHost());
            assertEquals(24225, tcpSender.failureDetector.getHeartbeater().getPort());

            tcpSender = (TCPSender) multiSender.senders.get(1);
            assertEquals("0.0.0.0", tcpSender.getHost());
            assertEquals(24226, tcpSender.getPort());
            assertEquals("0.0.0.0", tcpSender.failureDetector.getHeartbeater().getHost());
            assertEquals(24226, tcpSender.failureDetector.getHeartbeater().getPort());
        }
        finally {
            if (multiSender != null) {
                multiSender.close();
            }
        }
    }

    @Test
    public void testSend()
            throws IOException, InterruptedException
    {
        final MockMultiTCPServerWithMetrics server0 = new MockMultiTCPServerWithMetrics();
        server0.start();
        final MockMultiTCPServerWithMetrics server1 = new MockMultiTCPServerWithMetrics();
        server1.start();
        TimeUnit.MILLISECONDS.sleep(500);

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);

        final MultiSender sender = new MultiSender.Config(
                Arrays.<Sender.Config>asList(
                        new TCPSender.Config()
                                .setPort(server0.getLocalPort())
                                .setHeartbeaterConfig(
                                        new UDPHeartbeater.Config()
                                            .setPort(server0.getLocalPort())),
                        new TCPSender.Config()
                                .setPort(server1.getLocalPort())
                                .setHeartbeaterConfig(
                                        new UDPHeartbeater.Config()
                                                .setPort(server1.getLocalPort()))
                                )).createInstance();
        final ExecutorService senderExecutorService = Executors.newCachedThreadPool();
        final AtomicBoolean shouldFailOver = new AtomicBoolean(true);
        for (int i = 0; i < concurency; i++) {
            senderExecutorService.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        byte[] bytes = "0123456789".getBytes(Charset.forName("UTF-8"));

                        for (int j = 0; j < reqNum; j++) {
                            if (j == reqNum / 2) {
                                if (shouldFailOver.getAndSet(false)) {
                                    TimeUnit.MILLISECONDS.sleep(100);
                                    LOG.info("Failing over...");
                                    server0.stop();
                                }
                            }
                            sender.send(ByteBuffer.wrap(bytes));
                        }
                        latch.countDown();
                    }
                    catch (Exception e) {
                        LOG.error("Failed to send", e);
                    }
                }
            });
        }

        for (int i = 0; i < 60; i++) {
            if (latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }
        assertEquals(0, latch.getCount());

        sender.close();
        TimeUnit.MILLISECONDS.sleep(1000);
        server1.stop();
        TimeUnit.MILLISECONDS.sleep(1000);

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
        assertTrue(((long)(concurency - 1) * reqNum) * 10 <= recvLen && recvLen <= ((long)concurency * reqNum) * 10);
        assertEquals(2, closeCount);
    }
}