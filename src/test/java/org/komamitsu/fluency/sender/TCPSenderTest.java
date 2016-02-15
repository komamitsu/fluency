package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.MockTCPServer;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TCPSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSenderTest.class);

    @Test
    public void testSend()
            throws IOException, InterruptedException
    {
        MockTCPServerWithMetrics server = new MockTCPServerWithMetrics();
        server.start();

        int concurency = 20;
        final int reqNum = 5000;
        final CountDownLatch latch = new CountDownLatch(concurency);
        final TCPSender sender = new TCPSender.Config().setPort(server.getLocalPort()).createInstance();
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
        TimeUnit.MILLISECONDS.sleep(500);

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

        assertEquals(1, connectCount);
        assertEquals((long)concurency * reqNum * 10, recvLen);
        assertEquals(1, closeCount);
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
            throws IOException, InterruptedException
    {
        final MockTCPServer server = new MockTCPServer();
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
}