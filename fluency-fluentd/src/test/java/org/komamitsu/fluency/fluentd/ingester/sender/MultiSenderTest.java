/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.fluentd.ingester.sender;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.fluentd.MockTCPServerWithMetrics;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.UDPHeartbeater;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.komamitsu.fluency.fluentd.SSLTestSocketFactories.SSL_CLIENT_SOCKET_FACTORY;

class MultiSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSenderTest.class);

    private FailureDetector createFailureDetector(Heartbeater hb)
    {
        return new FailureDetector(new PhiAccrualFailureDetectStrategy(), hb);
    }

    @Test
    void testConstructorForTCPSender()
            throws IOException
    {
        MultiSender multiSender = null;
        try {
            TCPSender.Config senderConfig0 = new TCPSender.Config();
            senderConfig0.setPort(24225);

            TCPHeartbeater.Config hbConfig0 = new TCPHeartbeater.Config();
            hbConfig0.setPort(24225);

            TCPSender.Config senderConfig1 = new TCPSender.Config();
            senderConfig1.setHost("0.0.0.0");
            senderConfig1.setPort(24226);

            TCPHeartbeater.Config hbConfig1 = new TCPHeartbeater.Config();
            hbConfig1.setHost("0.0.0.0");
            hbConfig1.setPort(24226);

            multiSender = new MultiSender(new MultiSender.Config(),
                    Arrays.asList(
                            new TCPSender(senderConfig0,
                                    createFailureDetector(new TCPHeartbeater(hbConfig0))),
                            new TCPSender(senderConfig1,
                                    createFailureDetector(new TCPHeartbeater(hbConfig1)))));

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
    void testConstructorForSSLSender()
            throws IOException
    {
        MultiSender multiSender = null;
        try {

            SSLSender.Config senderConfig0 = new SSLSender.Config();
            senderConfig0.setPort(24225);
            senderConfig0.setSslSocketFactory(SSL_CLIENT_SOCKET_FACTORY);

            SSLHeartbeater.Config hbConfig0 = new SSLHeartbeater.Config();
            hbConfig0.setPort(24225);

            SSLSender.Config senderConfig1 = new SSLSender.Config();
            senderConfig1.setHost("0.0.0.0");
            senderConfig1.setPort(24226);
            senderConfig1.setSslSocketFactory(SSL_CLIENT_SOCKET_FACTORY);

            SSLHeartbeater.Config hbConfig1 = new SSLHeartbeater.Config();
            hbConfig1.setHost("0.0.0.0");
            hbConfig1.setPort(24226);

            multiSender = new MultiSender(new MultiSender.Config(),
                    Arrays.asList(
                            new SSLSender(senderConfig0,
                                    createFailureDetector(new SSLHeartbeater(hbConfig0))),
                            new SSLSender(senderConfig1,
                                    createFailureDetector(new SSLHeartbeater(hbConfig1)))));

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
    void testTCPSend()
            throws Exception
    {
        testSend(false);
    }

    @Test
    void testSSLSend()
            throws Exception
    {
        testSend(true);
    }

    private void testSend(boolean sslEnabled)
            throws Exception
    {
        final MockMultiTCPServerWithMetrics server0 = new MockMultiTCPServerWithMetrics(sslEnabled);
        server0.start();
        final MockMultiTCPServerWithMetrics server1 = new MockMultiTCPServerWithMetrics(sslEnabled);
        server1.start();

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);

        final MultiSender sender;

        if (sslEnabled) {
            SSLSender.Config senderConfig0 = new SSLSender.Config();
            senderConfig0.setPort(server0.getLocalPort());
            senderConfig0.setReadTimeoutMilli(500);
            senderConfig0.setSslSocketFactory(SSL_CLIENT_SOCKET_FACTORY);

            UDPHeartbeater.Config hbConfig0 = new UDPHeartbeater.Config();
            hbConfig0.setPort(server0.getLocalPort());

            SSLSender.Config senderConfig1 = new SSLSender.Config();
            senderConfig1.setPort(server1.getLocalPort());
            senderConfig1.setReadTimeoutMilli(500);
            senderConfig1.setSslSocketFactory(SSL_CLIENT_SOCKET_FACTORY);

            UDPHeartbeater.Config hbConfig1 = new UDPHeartbeater.Config();
            hbConfig1.setPort(server1.getLocalPort());

            sender = new MultiSender(new MultiSender.Config(),
                    ImmutableList.of(
                            new SSLSender(senderConfig0,
                                    createFailureDetector(new UDPHeartbeater(hbConfig0))),
                            new SSLSender(senderConfig1,
                                    createFailureDetector(new UDPHeartbeater(hbConfig1)))));
        }
        else {
            TCPSender.Config senderConfig0 = new TCPSender.Config();
            senderConfig0.setPort(server0.getLocalPort());
            senderConfig0.setReadTimeoutMilli(500);

            UDPHeartbeater.Config hbConfig0 = new UDPHeartbeater.Config();
            hbConfig0.setPort(server0.getLocalPort());

            TCPSender.Config senderConfig1 = new TCPSender.Config();
            senderConfig1.setPort(server1.getLocalPort());
            senderConfig1.setReadTimeoutMilli(500);

            UDPHeartbeater.Config hbConfig1 = new UDPHeartbeater.Config();
            hbConfig1.setPort(server0.getLocalPort());

            sender = new MultiSender(new MultiSender.Config(),
                    ImmutableList.of(
                            new TCPSender(senderConfig0,
                                    createFailureDetector(new UDPHeartbeater(hbConfig0))),
                            new TCPSender(senderConfig1,
                                    createFailureDetector(new UDPHeartbeater(hbConfig0)))));
        }

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
            fail("Sending all requests is timed out");
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
        // But w/o ack responses, Sender can't detect dropped requests. So some margin for expected result is allowed here.
        long minExpectedRecvLen = ((long) (concurency * (sslEnabled ? 0.5 : 0.8)) * reqNum) * 10;
        long maxExpectedRecvLen = ((long) concurency * reqNum) * 10;
        assertThat(recvLen, is(greaterThanOrEqualTo(minExpectedRecvLen)));
        assertThat(recvLen, is(lessThanOrEqualTo(maxExpectedRecvLen)));
        assertEquals(1, closeCount);
    }
}
