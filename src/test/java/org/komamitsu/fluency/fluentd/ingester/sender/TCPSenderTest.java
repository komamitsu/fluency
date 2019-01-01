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
import org.junit.Test;
import org.komamitsu.fluency.fluentd.MockTCPServer;
import org.komamitsu.fluency.fluentd.MockTCPServerWithMetrics;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class TCPSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSenderTest.class);

    interface TCPSenderConfigurator
    {
        TCPSender.Config config(int port);
    }

    @Test
    public void testSend()
            throws Exception
    {
        testSendBase(new TCPSenderConfigurator() {
            @Override
            public TCPSender.Config config(int port)
            {
                return new TCPSender.Config().setPort(port);
            }
        }, is(1), is(1));
    }

    @Test
    public void testSendWithHeartbeart()
            throws Exception
    {
        testSendBase(new TCPSenderConfigurator() {
            @Override
            public TCPSender.Config config(int port)
            {
                TCPHeartbeater.Config hbConfig = new TCPHeartbeater.Config().setPort(port).setIntervalMillis(400);
                return new TCPSender.Config().setPort(port).setHeartbeaterConfig(hbConfig);
            }
        }, greaterThan(1), greaterThan(1));
    }

    private void testSendBase(TCPSenderConfigurator configurator, Matcher connectCountMatcher, Matcher closeCountMatcher)
            throws Exception
    {
        MockTCPServerWithMetrics server = new MockTCPServerWithMetrics(false);
        server.start();

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);
        TCPSender.Config config = configurator.config(server.getLocalPort());
        final TCPSender sender = config.createInstance();

        // To receive heartbeat at least once
        TimeUnit.MILLISECONDS.sleep(500);

        final ExecutorService senderExecutorService = Executors.newCachedThreadPool();
        for (int i = 0; i < concurency; i++) {
            senderExecutorService.execute(new Runnable()
            {
                @Override
                public void run()
                {
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
        assertThat(recvLen, is((long)concurency * reqNum * 10));
        assertThat(closeCount, closeCountMatcher);
    }

    @Test
    public void testConnectionTimeout()
            throws IOException, InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable() {
            @Override
            public void run()
            {
                TCPSender sender = new TCPSender.Config().setHost("192.0.2.0").setConnectionTimeoutMilli(1000).createInstance();
                try {
                    sender.send(ByteBuffer.wrap("hello, world".getBytes("UTF-8")));
                }
                catch (Exception e) {
                    if (e instanceof SocketTimeoutException) {
                        latch.countDown();
                    }
                }
            }
        });
        assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testReadTimeout()
            throws Exception
    {
        final MockTCPServer server = new MockTCPServer(false);
        server.start();

        try {
            final CountDownLatch latch = new CountDownLatch(1);
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    TCPSender sender = new TCPSender.Config().setPort(server.getLocalPort()).setReadTimeoutMilli(1000).createInstance();
                    try {
                        sender.sendWithAck(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes("UTF-8"))), "Waiting ack forever".getBytes("UTF-8"));
                    }
                    catch (Exception e) {
                        if (e instanceof SocketTimeoutException) {
                            latch.countDown();
                        }
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
    public void testClose()
            throws Exception
    {
        final MockTCPServer server = new MockTCPServer(false);
        server.start();

        try {
            final AtomicLong duration = new AtomicLong();
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Void> future = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    TCPSender sender = new TCPSender.Config().setPort(server.getLocalPort()).setWaitBeforeCloseMilli(1500).createInstance();
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
                }
            });
            future.get(3000, TimeUnit.MILLISECONDS);
            assertTrue(duration.get() > 1000 && duration.get() < 2000);
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testConfig()
    {
        TCPSender.Config config = new TCPSender.Config();
        assertEquals(1000, config.getWaitBeforeCloseMilli());
        // TODO: Add others later
    }
}