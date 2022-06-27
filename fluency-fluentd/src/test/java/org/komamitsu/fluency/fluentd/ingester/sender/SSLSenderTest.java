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

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.fluentd.MockTCPServer;
import org.komamitsu.fluency.fluentd.MockTCPServerWithMetrics;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SSLSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(SSLSenderTest.class);
    private static final SSLSocketFactory SSL_SOCKET_FACTORY = new SSLTestClientSocketFactory().create();

    @Test
    void testSend()
            throws Exception
    {
        testSendBase(port -> {
            SSLSender.Config config = new SSLSender.Config();
            config.setPort(port);
            config.setSslSocketFactory(SSL_SOCKET_FACTORY);
            return new SSLSender(config);
        }, is(1), is(1));
    }

    @Test
    void testSendWithHeartbeart()
            throws Exception
    {
        testSendBase(port -> {
            SSLHeartbeater.Config hbConfig = new SSLHeartbeater.Config();
            hbConfig.setPort(port);
            hbConfig.setIntervalMillis(400);
            SSLSender.Config senderConfig = new SSLSender.Config();
            senderConfig.setPort(port);
            senderConfig.setSslSocketFactory(SSL_SOCKET_FACTORY);
            return new SSLSender(senderConfig,
                    new FailureDetector(
                            new PhiAccrualFailureDetectStrategy(),
                            new SSLHeartbeater(hbConfig)));
        }, greaterThan(1), greaterThan(1));
    }

    private void testSendBase(
            SSLSenderCreator sslSenderCreator,
            Matcher<? super Integer> connectCountMatcher,
            Matcher<? super Integer> closeCountMatcher)
            throws Exception
    {
        MockTCPServerWithMetrics server = new MockTCPServerWithMetrics(true);
        server.start();

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);
        SSLSender sender = sslSenderCreator.create(server.getLocalPort());

        // To receive heartbeat at least once
        TimeUnit.MILLISECONDS.sleep(500);

        final ExecutorService senderExecutorService = Executors.newCachedThreadPool();
        for (int i = 0; i < concurency; i++) {
            senderExecutorService.execute(() -> {
                try {
                    byte[] bytes = "0123456789".getBytes(Charset.forName("UTF-8"));

                    for (int j = 0; j < reqNum; j++) {
                        sender.send(ByteBuffer.wrap(bytes));
                    }
                    latch.countDown();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        if (!latch.await(30, TimeUnit.SECONDS)) {
            fail("Sending all requests timed out");
        }
        sender.close();

        server.waitUntilEventsStop();
        server.stop();

        int connectCount = 0;
        int closeCount = 0;
        long recvCount = 0;
        long recvLen = 0;
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
        LOG.debug("recvCount={}", recvCount);

        assertThat(connectCount, connectCountMatcher);
        assertThat(recvLen, is((long) concurency * reqNum * 10));
        assertThat(closeCount, closeCountMatcher);
    }

    @Test
    void testConnectionTimeout()
            throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            SSLSender.Config senderConfig = new SSLSender.Config();
            senderConfig.setHost("192.0.2.0");
            senderConfig.setConnectionTimeoutMilli(1000);
            senderConfig.setSslSocketFactory(SSL_SOCKET_FACTORY);
            SSLSender sender = new SSLSender(senderConfig);
            try {
                sender.send(ByteBuffer.wrap("hello, world".getBytes("UTF-8")));
            }
            catch (Throwable e) {
                if (e instanceof SocketTimeoutException) {
                    latch.countDown();
                }
                else {
                    throw new RuntimeException(e);
                }
            }
        });
        assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
    }

    @Test
    void testReadTimeout()
            throws Exception
    {
        final MockTCPServer server = new MockTCPServer(true);
        server.start();

        try {
            final CountDownLatch latch = new CountDownLatch(1);
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.execute(() -> {
                SSLSender.Config senderConfig = new SSLSender.Config();
                senderConfig.setPort(server.getLocalPort());
                senderConfig.setReadTimeoutMilli(1000);
                senderConfig.setSslSocketFactory(SSL_SOCKET_FACTORY);
                SSLSender sender = new SSLSender(senderConfig);
                try {
                    sender.sendWithAck(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8))), "Waiting ack forever");
                }
                catch (Throwable e) {
                    if (e instanceof SocketTimeoutException) {
                        latch.countDown();
                    }
                    else {
                        throw new RuntimeException(e);
                    }
                }
            });
            assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testClose()
            throws Exception
    {
        final MockTCPServer server = new MockTCPServer(true);
        server.start();

        try {
            final AtomicLong duration = new AtomicLong();
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Void> future = executorService.submit(() -> {
                SSLSender.Config senderConfig = new SSLSender.Config();
                senderConfig.setPort(server.getLocalPort());
                senderConfig.setWaitBeforeCloseMilli(1500);
                senderConfig.setSslSocketFactory(SSL_SOCKET_FACTORY);
                SSLSender sender = new SSLSender(senderConfig);
                long start;
                try {
                    sender.send(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes("UTF-8"))));
                    start = System.currentTimeMillis();
                    sender.close();
                    duration.set(System.currentTimeMillis() - start);
                }
                catch (Exception e) {
                    LOG.error("Unexpected exception", e);
                }

                return null;
            });
            future.get(3000, TimeUnit.MILLISECONDS);
            assertTrue(duration.get() > 1000 && duration.get() < 2000);
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testConfig()
    {
        SSLSender.Config config = new SSLSender.Config();
        assertEquals(1000, config.getWaitBeforeCloseMilli());
        // TODO: Add others later
    }

    @Test
    void validateConfig()
    {
        {
            SSLSender.Config config = new SSLSender.Config();
            config.setConnectionTimeoutMilli(9);
            assertThrows(IllegalArgumentException.class, () -> new SSLSender(config));
        }

        {
            SSLSender.Config config = new SSLSender.Config();
            config.setReadTimeoutMilli(9);
            assertThrows(IllegalArgumentException.class, () -> new SSLSender(config));
        }

        {
            SSLSender.Config config = new SSLSender.Config();
            config.setWaitBeforeCloseMilli(-1);
            assertThrows(IllegalArgumentException.class, () -> new SSLSender(config));
        }
    }

    interface SSLSenderCreator
    {
        SSLSender create(int port);
    }
}