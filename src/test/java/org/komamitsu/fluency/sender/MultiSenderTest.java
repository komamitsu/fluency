package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.sender.heartbeat.SSLHeartbeater;
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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;

public class MultiSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSenderTest.class);

    @Test
    public void testConstructorForTCPSender()
            throws IOException
    {
        MultiSender multiSender = null;
        try {
            multiSender = new MultiSender.Config(
                    Arrays.<Sender.Instantiator>asList(
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

            assertThat(multiSender.toString().length(), greaterThan(0));

            assertEquals(2, multiSender.getSenders().size());

            TCPSender tcpSender = (TCPSender) multiSender.getSenders().get(0);
            assertEquals("127.0.0.1", tcpSender.getHost());
            assertEquals(24225, tcpSender.getPort());
            assertEquals("127.0.0.1", tcpSender.getFailureDetector().getHeartbeater().getHost());
            assertEquals(24225, tcpSender.getFailureDetector().getHeartbeater().getPort());

            tcpSender = (TCPSender) multiSender.getSenders().get(1);
            assertEquals("0.0.0.0", tcpSender.getHost());
            assertEquals(24226, tcpSender.getPort());
            assertEquals("0.0.0.0", tcpSender.getFailureDetector().getHeartbeater().getHost());
            assertEquals(24226, tcpSender.getFailureDetector().getHeartbeater().getPort());
        }
        finally {
            if (multiSender != null) {
                multiSender.close();
            }
        }
    }

    @Test
    public void testConstructorForSSLSender()
            throws IOException
    {
        MultiSender multiSender = null;
        try {
            multiSender = new MultiSender.Config(
                    Arrays.<Sender.Instantiator>asList(
                            new SSLSender.Config()
                                    .setPort(24225)
                                    .setHeartbeaterConfig(
                                            new SSLHeartbeater.Config().
                                                    setPort(24225)),
                            new SSLSender.Config()
                                    .setHost("0.0.0.0")
                                    .setPort(24226)
                                    .setHeartbeaterConfig(
                                            new SSLHeartbeater.Config()
                                                    .setHost("0.0.0.0")
                                                    .setPort(24226))
                                    )).createInstance();

            assertThat(multiSender.toString().length(), greaterThan(0));

            assertEquals(2, multiSender.getSenders().size());

            SSLSender sslSender = (SSLSender) multiSender.getSenders().get(0);
            assertEquals("127.0.0.1", sslSender.getHost());
            assertEquals(24225, sslSender.getPort());
            assertEquals("127.0.0.1", sslSender.getFailureDetector().getHeartbeater().getHost());
            assertEquals(24225, sslSender.getFailureDetector().getHeartbeater().getPort());

            sslSender = (SSLSender) multiSender.getSenders().get(1);
            assertEquals("0.0.0.0", sslSender.getHost());
            assertEquals(24226, sslSender.getPort());
            assertEquals("0.0.0.0", sslSender.getFailureDetector().getHeartbeater().getHost());
            assertEquals(24226, sslSender.getFailureDetector().getHeartbeater().getPort());
        }
        finally {
            if (multiSender != null) {
                multiSender.close();
            }
        }
    }

    @Test
    public void testTCPSend()
            throws Exception
    {
        testSend(false);
    }

    @Test
    public void testSSLSend()
            throws Exception
    {
        testSend(true);
    }

    private void testSend(boolean useSsl)
            throws Exception
    {
        final MockMultiTCPServerWithMetrics server0 = new MockMultiTCPServerWithMetrics(useSsl);
        server0.start();
        final MockMultiTCPServerWithMetrics server1 = new MockMultiTCPServerWithMetrics(useSsl);
        server1.start();

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);

        final MultiSender sender = new MultiSender.Config(
                useSsl ?
                        Arrays.<Sender.Instantiator>asList(
                                new SSLSender.Config()
                                        .setPort(server0.getLocalPort())
                                        .setReadTimeoutMilli(500)
                                        .setHeartbeaterConfig(
                                                new UDPHeartbeater.Config()
                                                        .setPort(server0.getLocalPort())),
                                new SSLSender.Config()
                                        .setPort(server1.getLocalPort())
                                        .setReadTimeoutMilli(500)
                                        .setHeartbeaterConfig(
                                                new UDPHeartbeater.Config()
                                                        .setPort(server1.getLocalPort()))
                        ) :
                        Arrays.<Sender.Instantiator>asList(
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
                        )
        ).createInstance();
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

        if (!latch.await(60, TimeUnit.SECONDS)) {
            assertTrue("Sending all requests is timed out", false);
        }

        sender.close();

        server1.waitUntilEventsStop();
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
        // This test doesn't use actual PackedForward format so that it can simply test MultiSender itself.
        // But w/o ack responses, Sender can't detect dropped requests. So some margin for expected result is allowed here
        long minExpectedRecvLen = ((long)(concurency - (useSsl ? 4 : 1)) * reqNum) * 10;
        long maxExpectedRecvLen = ((long)concurency * reqNum) * 10;
        assertThat(recvLen, is(greaterThanOrEqualTo(minExpectedRecvLen)));
        assertThat(recvLen, is(lessThanOrEqualTo(maxExpectedRecvLen)));
        assertEquals(1, closeCount);
    }
}