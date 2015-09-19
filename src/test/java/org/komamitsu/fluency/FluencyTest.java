package org.komamitsu.fluency;

import org.junit.Test;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FluencyTest
{
    private static class EmitTask implements Runnable
    {
        private final Fluency fluency;
        private final String tag;
        private Map<String, Object> data;
        private final int count;
        private final CountDownLatch latch;

        private EmitTask(Fluency fluency, String tag, Map<String, Object> data, int count, CountDownLatch latch)
        {
            this.fluency = fluency;
            this.tag = tag;
            this.data = data;
            this.count = count;
            this.latch = latch;
        }

        @Override
        public void run()
        {
            for (int i = 0; i < count; i++) {
                try {
                    fluency.emit(tag, data);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Failed", e);
                }
            }
            latch.countDown();
        }
    }

    @Test
    public void test()
            throws Exception
    {
        // Fluency fluency = Fluency.defaultFluency("127.0.0.1", 24224);
        // Buffer buffer = new PackedForwardBuffer(new PackedForwardBuffer.Config().setBufferSize(256 * 1024 * 1024));
        Buffer buffer = new PackedForwardBuffer();
        Sender sender = new TCPSender("127.0.0.1", 24224);
        // Flusher flusher = new AsyncFlusher(buffer, sender, new FlusherConfig().setFlushIntervalMillis(100));
        Flusher flusher = new AsyncFlusher(buffer, sender);
        final Fluency fluency = new Fluency.Builder(sender).setBuffer(buffer).
                setFlusher(new AsyncFlusher(buffer, sender)).build();
        try {
            final Map<String, Object> hashMap = new HashMap<String, Object>();
            hashMap.put("name", "komamitsu");
            hashMap.put("age", 42);
            hashMap.put("email", "komamitsu@gmail.com");

            int concurrency = 4;
            long start = System.currentTimeMillis();
            final CountDownLatch latch = new CountDownLatch(concurrency);
            ExecutorService es = Executors.newCachedThreadPool();
            for (int i = 0; i < concurrency; i++) {
                // String tag = String.format("foodb%d.bartbl%d", i, i);
                String tag = "foodb.bartbl";
                es.execute(new EmitTask(fluency, tag, hashMap, 1000000, latch));
            }
            latch.await(30, TimeUnit.SECONDS);
            if (latch.getCount() != 0) {
                assertTrue(false);
            }
            System.out.println(System.currentTimeMillis() - start);
        } finally {
            fluency.close();
        }
    }
}