package org.komamitsu.fluency.buffer;

import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.sender.MockTCPSender;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MessageBufferTest
{
    private String longStr;

    @Before
    public void setup()
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            stringBuilder.append("xxxxxxxxxx");
        }
        longStr = stringBuilder.toString();
    }

    public void baseTestMessageBuffer(final int loopCount, final boolean multiTags, final boolean syncFlush, final Buffer buffer)
            throws IOException, InterruptedException
    {
        final int concurrency = 4;
        final CountDownLatch latch = new CountDownLatch(concurrency);

        final MockTCPSender sender = new MockTCPSender(24229);

        Runnable emitTask = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    for (int i = 0; i < loopCount; i++) {
                        HashMap<String, Object> data = new HashMap<String, Object>();
                        data.put("name", String.format("komamitsu%06d", i));
                        data.put("age", i);
                        data.put("comment", i % 31 == 0 ? longStr : "hello");
                        String tag = multiTags ? String.format("foodb%d.bartbl%d", i % 4, i % 4) : "foodb.bartbl";
                        buffer.append(tag, System.currentTimeMillis(), data);

                        if (syncFlush) {
                            if (i % 20 == 0) {
                                buffer.flush(sender);
                            }
                        }
                    }
                    buffer.flush(sender);
                    buffer.close();
                    latch.countDown();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        final ExecutorService flushService = Executors.newSingleThreadExecutor();
        if (!syncFlush) {
            flushService.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    while (!flushService.isShutdown()) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(100L);
                            buffer.flush(sender);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        catch (IOException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                }
            });
        }

        long start = System.currentTimeMillis();
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executorService.execute(emitTask);
        }
        latch.await(10, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        assertEquals(0, latch.getCount());

        flushService.shutdown();
        flushService.awaitTermination(10, TimeUnit.SECONDS);

        int totalLoopCount = concurrency * loopCount;

        assertEquals(totalLoopCount, sender.getEvents().size());

        HashMap<String, Integer> tagCounts = new HashMap<String, Integer>();
        String minName = "zzzzzzzzzzzzzzzzzzzz";
        String maxName = "";
        int minAge = Integer.MAX_VALUE;
        int maxAge = Integer.MIN_VALUE;
        int longCommentCount = 0;
        for (Object event : sender.getEvents()) {
            assertTrue(event instanceof List);
            List<Object> items = (List<Object>) event;
            assertTrue(items.get(0) instanceof String);
            assertTrue(items.get(1) instanceof Long);
            assertTrue(items.get(2) instanceof Map);
            String tag = (String) items.get(0);
            long timstamp = (Long)items.get(1);
            Map<String, Object> data = (Map<String, Object>) items.get(2);

            Integer count = tagCounts.get(tag);
            if (count == null) {
                count = 0;
            }
            tagCounts.put(tag, count + 1);

            if (start <= timstamp && timstamp <= end) {}
            else {
                System.out.println("timestamp=" + timstamp);
            }
            assertTrue(start <= timstamp && timstamp <= end);

            assertEquals(3, data.size());
            String name = (String) data.get("name");
            int age = (Integer) data.get("age");
            String comment = (String) data.get("comment");
            if (name.compareTo(minName) < 0) {
                minName = name;
            }
            if (name.compareTo(maxName) > 0) {
                maxName = name;
            }
            if (age < minAge) {
                minAge = age;
            }
            if (age > maxAge) {
                maxAge = age;
            }

            if (comment.equals("hello")) {
            }
            else if (comment.equals(longStr)) {
                longCommentCount++;
            }
            else {
                assertTrue(false);
            }
        }

        if (multiTags) {
            assertEquals(4, tagCounts.size());
            for (int i = 0; i < 4; i++) {
                int count = tagCounts.get(String.format("foodb%d.bartbl%d", i, i));
                assertTrue(totalLoopCount / 4 - 2 < count && count < totalLoopCount / 4 + 2);
            }
        }
        else {
            assertEquals(1, tagCounts.size());
            int count = tagCounts.get("foodb.bartbl");
            assertEquals(totalLoopCount, count);
        }

        assertEquals("komamitsu000000", minName);
        assertEquals(String.format("komamitsu%06d", loopCount - 1), maxName);
        assertEquals(0, minAge);
        assertEquals(loopCount - 1, maxAge);

        assertTrue(totalLoopCount / 31 - 6 < longCommentCount && longCommentCount < totalLoopCount / 31 + 6);
    }

    @Test
    public void testMessageBuffer()
            throws IOException, InterruptedException
    {
        for (Integer loopCount : Arrays.asList(100, 1000, 10000)) {
            baseTestMessageBuffer(loopCount, true, true, new MessageBuffer(new MessageBuffer.Config()));
            baseTestMessageBuffer(loopCount, false, true, new MessageBuffer(new MessageBuffer.Config()));
            baseTestMessageBuffer(loopCount, true, false, new MessageBuffer(new MessageBuffer.Config()));
            baseTestMessageBuffer(loopCount, false, false, new MessageBuffer(new MessageBuffer.Config()));
        }
    }
}