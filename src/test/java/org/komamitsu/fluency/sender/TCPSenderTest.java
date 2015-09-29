package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
        final TCPSender sender = new TCPSender(server.getLocalPort());
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
}