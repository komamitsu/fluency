/*
 * Copyright 2022 Mitsunori Komatsu (komamitsu)
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.fluentd.MockUnixSocketServer;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.UnixSocketHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

class UnixSocketSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(UnixSocketSenderTest.class);


    @Test
    void testSend()
            throws Exception
    {
        testSendBase(socketPath -> {
            UnixSocketSender.Config senderConfig = new UnixSocketSender.Config();
            senderConfig.setPath(socketPath);
            return new UnixSocketSender(senderConfig);
        }, is(1), is(1));
    }

    @Test
    void testSendWithHeartbeart()
            throws Exception
    {
        testSendBase(socketPath -> {
            UnixSocketHeartbeater.Config hbConfig = new UnixSocketHeartbeater.Config();
            hbConfig.setPath(socketPath);
            hbConfig.setIntervalMillis(400);

            UnixSocketSender.Config senderConfig = new UnixSocketSender.Config();
            senderConfig.setPath(socketPath);

            return new UnixSocketSender(senderConfig,
                    new FailureDetector(
                            new PhiAccrualFailureDetectStrategy(),
                            new UnixSocketHeartbeater(hbConfig)));
        }, greaterThan(1), greaterThan(1));
    }

    private void testSendBase(
            SenderCreator senderCreator,
            Matcher<? super Integer> connectCountMatcher,
            Matcher<? super Integer> closeCountMatcher)
            throws Exception
    {
        try (MockUnixSocketServer server = new MockUnixSocketServer()) {
            server.start();

            int concurency = 20;
            final int reqNum = 5000;
            final CountDownLatch latch = new CountDownLatch(concurency);
            UnixSocketSender sender = senderCreator.create(server.getSocketPath());

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
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            assertTrue(latch.await(4, TimeUnit.SECONDS));
            sender.close();

            server.waitUntilEventsStop();
            server.stop();

            int connectCount = 0;
            int closeCount = 0;
            long recvCount = 0;
            long recvLen = 0;
            for (Tuple<MockUnixSocketServer.Type, Integer> event : server.getEvents()) {
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
    }

    @Test
    void testReadTimeout()
            throws Exception
    {
        try (MockUnixSocketServer server = new MockUnixSocketServer()) {
            server.start();

            try {
                final CountDownLatch latch = new CountDownLatch(1);
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                executorService.execute(() -> {
                    UnixSocketSender.Config senderConfig = new UnixSocketSender.Config();
                    senderConfig.setPath(server.getSocketPath());
                    senderConfig.setReadTimeoutMilli(1000);
                    UnixSocketSender sender = new UnixSocketSender(senderConfig);
                    try {
                        sender.sendWithAck(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8))), "Waiting ack forever");
                    } catch (Throwable e) {
                        if (e instanceof SocketTimeoutException) {
                            latch.countDown();
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                });
                assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
            } finally {
                server.stop();
            }
        }
    }

    private Throwable extractRootCause(Throwable exception)
    {
        Throwable e = exception;
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    @Test
    void testDisconnBeforeRecv()
            throws Exception
    {
        try (MockUnixSocketServer server = new MockUnixSocketServer()) {
            server.start();

            try {
                final CountDownLatch latch = new CountDownLatch(1);
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                executorService.execute(() -> {
                    UnixSocketSender.Config senderConfig = new UnixSocketSender.Config();
                    senderConfig.setPath(server.getSocketPath());
                    senderConfig.setReadTimeoutMilli(4000);
                    UnixSocketSender sender = new UnixSocketSender(senderConfig);
                    try {
                        sender.sendWithAck(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8))), "Waiting ack forever");
                    } catch (Throwable e) {
                        Throwable rootCause = extractRootCause(e);
                        if (rootCause instanceof SocketException && rootCause.getMessage().toLowerCase().contains("disconnected")) {
                            latch.countDown();
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                });

                TimeUnit.MILLISECONDS.sleep(1000);
                server.stop(true);

                assertTrue(latch.await(8000, TimeUnit.MILLISECONDS));
            } finally {
                server.stop();
            }
        }
    }

    @Test
    void testClose()
            throws Exception
    {
        try (MockUnixSocketServer server = new MockUnixSocketServer()) {
            server.start();

            try {
                final AtomicLong duration = new AtomicLong();
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                Future<Void> future = executorService.submit(() -> {
                    UnixSocketSender.Config senderConfig = new UnixSocketSender.Config();
                    senderConfig.setPath(server.getSocketPath());
                    senderConfig.setWaitBeforeCloseMilli(1500);
                    UnixSocketSender sender = new UnixSocketSender(senderConfig);
                    long start;
                    try {
                        sender.send(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes("UTF-8"))));
                        start = System.currentTimeMillis();
                        sender.close();
                        duration.set(System.currentTimeMillis() - start);
                    } catch (Exception e) {
                        LOG.error("Unexpected exception", e);
                    }

                    return null;
                });
                future.get(3000, TimeUnit.MILLISECONDS);
                assertTrue(duration.get() > 1000 && duration.get() < 2000);
            } finally {
                server.stop();
            }
        }
    }

    @Test
    void testConfig()
    {
        UnixSocketSender.Config config = new UnixSocketSender.Config();
        assertEquals(1000, config.getWaitBeforeCloseMilli());
        // TODO: Add others later
    }

    @Test
    void validateConfig()
    {
        {
            UnixSocketSender.Config config = new UnixSocketSender.Config();
            config.setConnectionTimeoutMilli(9);
            assertThrows(IllegalArgumentException.class, () -> new UnixSocketSender(config));
        }

        {
            UnixSocketSender.Config config = new UnixSocketSender.Config();
            config.setReadTimeoutMilli(9);
            assertThrows(IllegalArgumentException.class, () -> new UnixSocketSender(config));
        }

        {
            UnixSocketSender.Config config = new UnixSocketSender.Config();
            config.setWaitBeforeCloseMilli(-1);
            assertThrows(IllegalArgumentException.class, () -> new UnixSocketSender(config));
        }
    }

    interface SenderCreator
    {
        UnixSocketSender create(Path socketPath);
    }
}
