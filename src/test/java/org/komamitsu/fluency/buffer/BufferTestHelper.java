package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.sender.MockTCPSender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ImmutableValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class BufferTestHelper
{
    private static final Logger LOG = LoggerFactory.getLogger(BufferTestHelper.class);
    private final String longStr;
    private HashMap<String, Integer> tagCounts;
    private String minName;
    private String maxName;
    private int minAge;
    private int maxAge;
    private int longCommentCount;

    public BufferTestHelper()
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            stringBuilder.append("xxxxxxxxxx");
        }
        longStr = stringBuilder.toString();
        tagCounts = new HashMap<String, Integer>();
        minName = "zzzzzzzzzzzzzzzzzzzz";
        maxName = "";
        minAge = Integer.MAX_VALUE;
        maxAge = Integer.MIN_VALUE;
        longCommentCount = 0;
    }

    public void baseTestMessageBuffer(final int loopCount, final boolean multiTags, final boolean syncFlush, final boolean eventTime, final Buffer buffer)
            throws IOException, InterruptedException
    {
        assertThat(buffer.getBufferUsage(), is(0f));
        assertThat(buffer.getAllocatedSize(), is(0L));
        assertThat(buffer.getBufferedDataSize(), is(0L));

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
                        if (eventTime) {
                            buffer.append(tag, new EventTime((int) (System.currentTimeMillis() / 1000), 999999999), data);
                        }
                        else {
                            buffer.append(tag, System.currentTimeMillis(), data);
                        }
                    }
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
                            buffer.flush(sender, false);
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
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertThat(buffer.getBufferUsage(), is(greaterThan(0f)));
        assertThat(buffer.getAllocatedSize(), is(greaterThan(0L)));
        assertThat(buffer.getBufferedDataSize(), is(greaterThan(0L)));

        buffer.flush(sender, true);
        buffer.close();
        long end = System.currentTimeMillis();

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        if (!executorService.isTerminated()) {
            executorService.shutdownNow();
        }
        flushService.shutdown();
        flushService.awaitTermination(10, TimeUnit.SECONDS);
        if (!flushService.isTerminated()) {
            flushService.shutdownNow();
        }
        buffer.close();     // Just in case
        assertThat(buffer.getBufferUsage(), is(0f));
        assertThat(buffer.getAllocatedSize(), is(0L));
        assertThat(buffer.getBufferedDataSize(), is(0L));

        int totalLoopCount = concurrency * loopCount;

        ByteBuffer headerBuffer = null;

        int recordCount = 0;
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        objectMapper.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
        for (ByteBuffer byteBuffer : sender.getEvents()) {
            if (headerBuffer == null) {
                headerBuffer = byteBuffer;
                continue;
            }
            byte[] bytes = new byte[headerBuffer.limit() + byteBuffer.limit()];
            headerBuffer.get(bytes, 0, headerBuffer.limit());
            byteBuffer.get(bytes, headerBuffer.limit(), byteBuffer.limit());
            headerBuffer = null;

            MessageUnpacker messageUnpacker = MessagePack.newDefaultUnpacker(bytes);
            assertEquals(3, messageUnpacker.unpackArrayHeader());

            String tag = messageUnpacker.unpackString();
            byte[] payload = messageUnpacker.readPayload(messageUnpacker.unpackBinaryHeader());
            messageUnpacker = MessagePack.newDefaultUnpacker(payload);
            while (messageUnpacker.hasNext()) {
                assertEquals(2, messageUnpacker.unpackArrayHeader());

                ImmutableValue timestamp = messageUnpacker.unpackValue();

                int size = messageUnpacker.unpackMapHeader();
                assertEquals(3, size);
                Map<String, Object> data = new HashMap<String, Object>();
                for (int i = 0; i < size; i++) {
                    String key = messageUnpacker.unpackString();
                    ImmutableValue value = messageUnpacker.unpackValue();
                    if (value.isStringValue()) {
                        data.put(key, value.asStringValue().asString());
                    }
                    else if (value.isIntegerValue()) {
                        data.put(key, value.asIntegerValue().asInt());
                    }
                }

                analyzeResult(tag, timestamp, data, start, end, eventTime);
                recordCount++;
            }
        }

        assertEquals(totalLoopCount, recordCount);

        if (multiTags) {
            assertEquals(4, tagCounts.size());
            for (int i = 0; i < 4; i++) {
                int count = tagCounts.get(String.format("foodb%d.bartbl%d", i, i));
                assertTrue(totalLoopCount / 4 - 4 <= count && count <= totalLoopCount / 4 + 4);
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

        assertTrue(totalLoopCount / 31 - 5 <= longCommentCount && longCommentCount <= totalLoopCount / 31 + 5);
    }

    private void analyzeResult(String tag, ImmutableValue timestamp, Map<String, Object> data, long start, long end, boolean eventTime)
    {
        Integer count = tagCounts.get(tag);
        if (count == null) {
            count = 0;
        }
        tagCounts.put(tag, count + 1);

        if (eventTime) {
            assertThat(timestamp.isExtensionValue(), is(true));
            ExtensionValue tsInEventTime = timestamp.asExtensionValue();
            assertThat(tsInEventTime.getType(), CoreMatchers.is((byte) 0x00));
            ByteBuffer secondsAndNanoSeconds = ByteBuffer.wrap(tsInEventTime.getData());
            int seconds = secondsAndNanoSeconds.getInt();
            int nanoSeconds = secondsAndNanoSeconds.getInt();
            assertTrue(start / 1000 <= seconds && seconds <= end / 1000);
            assertThat(nanoSeconds, is(999999999));
        }
        else {
            assertThat(timestamp.isIntegerValue(), is(true));
            long tsInEpochMilli = timestamp.asIntegerValue().asLong();
            assertTrue(start <= tsInEpochMilli && tsInEpochMilli<= end);
        }

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
            // expected
        }
        else if (comment.equals(longStr)) {
            longCommentCount++;
        }
        else {
            assertTrue(false);
        }
    }
}
