package org.komamitsu.fluency;

import org.junit.Test;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.MessageBuffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.flusher.SyncFlusher;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class FluencyTest
{
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTest.class);

    @Test
    public void testDefaultFluency()
            throws IOException
    {
        Fluency fluency = null;
        fluency = Fluency.defaultFluency();
        fluency = Fluency.defaultFluency(12345);
        fluency = Fluency.defaultFluency("333.333.333.333", 12345);
        fluency = Fluency.defaultFluency(Arrays.asList(new InetSocketAddress(43210)));
        Fluency.Config config = new Fluency.Config();
        config.setFlushIntervalMillis(200).setMaxBufferSize(64 * 1024 * 1024).setSenderMaxRetryCount(99);
        fluency = Fluency.defaultFluency(config);
        fluency = Fluency.defaultFluency(12345, config);
        fluency = Fluency.defaultFluency("333.333.333.333", 12345, config);
        fluency = Fluency.defaultFluency(Arrays.asList(new InetSocketAddress(43210)), config);
    }

    interface FluencyFactory
    {
        Fluency generate(int fluentdPort)
                throws IOException;
    }

    @Test
    public void testFluencyEachCombination()
            throws Exception
    {
        testFluencyUsingPackedForwardBufferAndAsyncFlusher();
        testFluencyUsingMessageAndAsyncFlusher();
        testFluencyUsingPackedForwardBufferAndSyncFlusher();
        testFluencyUsingMessageAndSyncFlusher();
        testFluencyUsingMessageAndSyncFlusherWithAckResponse();
        testFluencyUsingMessageAndAsyncFlusherWithAckResponse();
        testFluencyUsingPackedForwardAndSyncFlusherWithAckResponse();
        testFluencyUsingPackedForwardAndAsyncFlusherWithAckResponse();
    }

    public void testFluencyUsingPackedForwardBufferAndAsyncFlusher()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new PackedForwardBuffer.Config();
                Flusher.Config flusherConfig = new AsyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingMessageAndAsyncFlusher()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new MessageBuffer.Config();
                Flusher.Config flusherConfig = new AsyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingPackedForwardBufferAndSyncFlusher()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new PackedForwardBuffer.Config();
                Flusher.Config flusherConfig = new SyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingMessageAndSyncFlusher()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new MessageBuffer.Config();
                Flusher.Config flusherConfig = new SyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingMessageAndSyncFlusherWithAckResponse()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new MessageBuffer.Config().setAckResponseMode(true);
                Flusher.Config flusherConfig = new SyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingMessageAndAsyncFlusherWithAckResponse()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new MessageBuffer.Config().setAckResponseMode(true);
                Flusher.Config flusherConfig = new AsyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingPackedForwardAndSyncFlusherWithAckResponse()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new PackedForwardBuffer.Config().setAckResponseMode(true);
                Flusher.Config flusherConfig = new SyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyUsingPackedForwardAndAsyncFlusherWithAckResponse()
            throws Exception
    {
        testFluencyBase(new FluencyFactory() {
            @Override
            public Fluency generate(int fluentdPort)
                    throws IOException
            {
                Sender sender = new TCPSender(fluentdPort);
                Buffer.Config bufferConfig = new PackedForwardBuffer.Config().setAckResponseMode(true);
                Flusher.Config flusherConfig = new AsyncFlusher.Config();
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        });
    }

    public void testFluencyBase(FluencyFactory fluencyFactory)
            throws Exception
    {
        MockFluentdServer fluentd = new MockFluentdServer();
        fluentd.start();
        TimeUnit.MILLISECONDS.sleep(500);

        final Fluency fluency = fluencyFactory.generate(fluentd.getLocalPort());

        final int maxNameLen = 200;
        final HashMap<Integer, String> nameLenTable = new HashMap<Integer, String>(maxNameLen);
        for (int i = 1; i <= maxNameLen; i++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 0; j < i; j++) {
                stringBuilder.append('x');
            }
            nameLenTable.put(i, stringBuilder.toString());
        }

        final AtomicLong ageEventsSum = new AtomicLong();
        final AtomicLong nameEventsLength = new AtomicLong();
        final AtomicLong tag0EventsCounter = new AtomicLong();
        final AtomicLong tag1EventsCounter = new AtomicLong();
        final AtomicLong tag2EventsCounter = new AtomicLong();
        final AtomicLong tag3EventsCounter = new AtomicLong();

        try {
            final Random random = new Random();
            int concurrency = 10;
            final int reqNum = 6000;
            long start = System.currentTimeMillis();
            final CountDownLatch latch = new CountDownLatch(concurrency);
            ExecutorService es = Executors.newCachedThreadPool();
            for (int i = 0; i < concurrency; i++) {
                es.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (int i = 0; i < reqNum; i++) {
                            int tagNum = i % 4;
                            final String tag = String.format("foodb%d.bartbl%d", tagNum, tagNum);
                            switch (tagNum) {
                                case 0:
                                    tag0EventsCounter.incrementAndGet();
                                    break;
                                case 1:
                                    tag1EventsCounter.incrementAndGet();
                                    break;
                                case 2:
                                    tag2EventsCounter.incrementAndGet();
                                    break;
                                case 3:
                                    tag3EventsCounter.incrementAndGet();
                                    break;
                                default:
                                    throw new RuntimeException("Never reach here");
                            }

                            int rand = random.nextInt(maxNameLen);
                            final Map<String, Object> hashMap = new HashMap<String, Object>();
                            String name = nameLenTable.get(rand + 1);
                            nameEventsLength.addAndGet(name.length());
                            hashMap.put("name", name);
                            rand = random.nextInt(100);
                            int age = rand;
                            ageEventsSum.addAndGet(age);
                            hashMap.put("age", age);
                            hashMap.put("comment", "hello, world");
                            hashMap.put("rate", 1.23);
                            try {
                                fluency.emit(tag, hashMap);
                            }
                            catch (IOException e) {
                                e.printStackTrace();
                                throw new RuntimeException("Failed", e);
                            }
                        }
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
            fluency.flush();
            TimeUnit.MILLISECONDS.sleep(5000);
            fluentd.stop();
            TimeUnit.MILLISECONDS.sleep(5000);

            assertEquals(1, fluentd.connectCounter.get());
            assertEquals(1, fluentd.closeCounter.get());
            assertEquals((long)concurrency * reqNum, fluentd.ageEventsCounter.get());
            assertEquals(ageEventsSum.get(), fluentd.ageEventsSum.get());
            assertEquals((long)concurrency * reqNum, fluentd.nameEventsCounter.get());
            assertEquals(nameEventsLength.get(), fluentd.nameEventsLength.get());
            assertEquals(tag0EventsCounter.get(), fluentd.tag0EventsCounter.get());
            assertEquals(tag1EventsCounter.get(), fluentd.tag1EventsCounter.get());
            assertEquals(tag2EventsCounter.get(), fluentd.tag2EventsCounter.get());
            assertEquals(tag3EventsCounter.get(), fluentd.tag3EventsCounter.get());

            System.out.println(System.currentTimeMillis() - start);
        } finally {
            fluency.close();
            fluentd.stop();
        }
    }

    private static class MockFluentdServer extends AbstractFluentdServer
    {
        private AtomicLong connectCounter = new AtomicLong();
        private AtomicLong ageEventsCounter = new AtomicLong();
        private AtomicLong ageEventsSum = new AtomicLong();
        private AtomicLong nameEventsCounter = new AtomicLong();
        private AtomicLong nameEventsLength = new AtomicLong();
        private AtomicLong tag0EventsCounter = new AtomicLong();
        private AtomicLong tag1EventsCounter = new AtomicLong();
        private AtomicLong tag2EventsCounter = new AtomicLong();
        private AtomicLong tag3EventsCounter = new AtomicLong();
        private AtomicLong closeCounter = new AtomicLong();
        private final long startTimestamp;

        public MockFluentdServer()
                throws IOException
        {
            startTimestamp = System.currentTimeMillis() / 1000;
        }

        @Override
        protected EventHandler getFluentdEventHandler()
        {
            return new EventHandler()
            {
                @Override
                public void onConnect(SocketChannel accpetSocketChannel)
                {
                    connectCounter.incrementAndGet();
                }

                @Override
                public void onReceive(String tag, long timestampMillis, MapValue data)
                {
                    if (tag.equals("foodb0.bartbl0")) {
                        tag0EventsCounter.incrementAndGet();
                    }
                    else if (tag.equals("foodb1.bartbl1")) {
                        tag1EventsCounter.incrementAndGet();
                    }
                    else if (tag.equals("foodb2.bartbl2")) {
                        tag2EventsCounter.incrementAndGet();
                    }
                    else if (tag.equals("foodb3.bartbl3")) {
                        tag3EventsCounter.incrementAndGet();
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected tag: tag=" + tag);
                    }

                    assertTrue(startTimestamp <= timestampMillis && timestampMillis < startTimestamp + 60 * 1000);

                    assertEquals(4, data.size());
                    for (Map.Entry<Value, Value> kv : data.entrySet()) {
                        String key = kv.getKey().asStringValue().toString();
                        Value val = kv.getValue();
                        if (key.equals("comment")) {
                            assertEquals("hello, world", val.toString());
                        }
                        else if (key.equals("rate")) {
                            // Treating the value as String to avoid a failure of calling asFloatValue()...
                            assertEquals("1.23", val.toString());
                        }
                        else if (key.equals("name")) {
                            nameEventsCounter.incrementAndGet();
                            nameEventsLength.addAndGet(val.asRawValue().asString().length());
                        }
                        else if (key.equals("age")) {
                            ageEventsCounter.incrementAndGet();
                            ageEventsSum.addAndGet(val.asIntegerValue().asInt());
                        }
                    }
                }

                @Override
                public void onClose(SocketChannel accpetSocketChannel)
                {
                    closeCounter.incrementAndGet();
                }
            };
        }
    }

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

    // @Test
    public void testWithRealFluentd()
            throws IOException, InterruptedException
    {
        int concurrency = 4;
        int reqNum = 1000000;
        // Fluency fluency = Fluency.defaultFluency();
        TCPSender sender = new TCPSender();
        Buffer.Config bufferConfig = new PackedForwardBuffer.Config().setMaxBufferSize(256 * 1024 * 1024);
        Flusher.Config flusherConfig = new AsyncFlusher.Config().setFlushIntervalMillis(200);
        Fluency fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("name", "komamitsu");
        data.put("age", 42);
        data.put("comment", "hello, world");
        CountDownLatch latch = new CountDownLatch(concurrency);
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < concurrency; i++) {
            executorService.execute(new EmitTask(fluency, "foodb.bartbl", data, reqNum, latch));
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS));
        fluency.close();
    }

    // @Test
    public void testWithRealMultipleFluentd()
            throws IOException, InterruptedException
    {
        int concurrency = 4;
        int reqNum = 1000000;
        // Fluency fluency = Fluency.defaultFluency();
        Fluency fluency = Fluency.defaultFluency(Arrays.asList(new InetSocketAddress(24224), new InetSocketAddress(24225)));
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("name", "komamitsu");
        data.put("age", 42);
        data.put("comment", "hello, world");
        CountDownLatch latch = new CountDownLatch(concurrency);
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < concurrency; i++) {
            executorService.execute(new EmitTask(fluency, "foodb.bartbl", data, reqNum, latch));
        }
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        fluency.close();
    }
}