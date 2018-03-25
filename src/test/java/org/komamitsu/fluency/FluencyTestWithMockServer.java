package org.komamitsu.fluency;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.sender.MultiSender;
import org.komamitsu.fluency.sender.SSLSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Theories.class)
public class FluencyTestWithMockServer
{
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTestWithMockServer.class);
    private static final int SMALL_BUF_SIZE = 4 * 1024 * 1024;
    private static final String TMPDIR = System.getProperty("java.io.tmpdir");
    @DataPoints
    public static final Options[] OPTIONS = {
            // TCP
            new Options(false, false, false, false, false, false), // Normal
            new Options(true, false, false, false, false, false),  // Failover
            new Options(false, true, false, true, false, false),   // File backup + Ack response
            new Options(false, false, true, false, false, false),  // Close instead of flush
            new Options(false, false, false, true, false, false),  // Ack response
            new Options(false, false, false, false, true, false),  // Small buffer
            new Options(true, false, false, false, true, false),   // Failover + Small buffer
            new Options(false, false, true, false, true, false),   // Close instead of flush + Small buffer
            new Options(false, false, false, true, true, false),   // Ack response + Small buffer
            new Options(true, false, true, false, false, false),   // Failover + Close instead of flush
            new Options(false, true, true, true, false, false),    // File backup + Ack response + Close instead of flush
            new Options(false, false, true, true, false, false),   // Ack response + Close instead of flush
            new Options(false, false, false, false, false, false, EmitType.MAP_WITH_EVENT_TIME), // EmitType = MAP_WITH_EVENT_TIME
            new Options(false, false, false, false, false, false, EmitType.MSGPACK_MAP_VALUE_BYTES), // EmitType = MSGPACK_MAP_VALUE_BYTES
            new Options(false, false, false, false, false, false, EmitType.MSGPACK_MAP_VALUE_BYTES_WITH_EVENT_TIME), // EmitType = MSGPACK_MAP_VALUE_BYTES_WITH_EVENT_TIME
            new Options(false, false, false, false, false, false, EmitType.MSGPACK_MAP_VALUE_BYTEBUFFER), // EmitType = MSGPACK_MAP_VALUE_BYTEBUFFER
            new Options(false, false, false, false, false, false, EmitType.MSGPACK_MAP_VALUE_BYTEBUFFER_WITH_EVENT_TIME), // EmitType = MSGPACK_MAP_VALUE_BYTEBUFFER_WITH_EVENT_TIME
            // SSL
            new Options(false, false, false, false, false, true), // Normal
            new Options(true, false, false, false, false, true),  // Failover
            new Options(false, true, false, true, false, true),   // File backup + Ack response
            new Options(false, false, true, false, false, true),  // Close instead of flush
            new Options(false, false, false, true, false, true),  // Ack response
            new Options(false, false, false, false, true, true),  // Small buffer
            new Options(true, false, false, false, true, true),   // Failover + Small buffer
            new Options(false, false, true, false, true, true),   // Close instead of flush + Small buffer
            new Options(false, false, false, true, true, true),   // Ack response + Small buffer
            new Options(true, false, true, false, false, true),   // Failover + Close instead of flush
            new Options(false, true, true, true, false, true),    // File backup + Ack response + Close instead of flush
            new Options(false, false, true, true, false, true),   // Ack response + Close instead of flush
            new Options(false, false, false, false, false, true, EmitType.MAP_WITH_EVENT_TIME), // EmitType = MAP_WITH_EVENT_TIME
            new Options(false, false, false, false, false, true, EmitType.MSGPACK_MAP_VALUE_BYTES), // EmitType = MSGPACK_MAP_VALUE_BYTES
            new Options(false, false, false, false, false, true, EmitType.MSGPACK_MAP_VALUE_BYTES_WITH_EVENT_TIME), // EmitType = MSGPACK_MAP_VALUE_BYTES_WITH_EVENT_TIME
            new Options(false, false, false, false, false, true, EmitType.MSGPACK_MAP_VALUE_BYTEBUFFER), // EmitType = MSGPACK_MAP_VALUE_BYTEBUFFER
            new Options(false, false, false, false, false, true, EmitType.MSGPACK_MAP_VALUE_BYTEBUFFER_WITH_EVENT_TIME), // EmitType = MSGPACK_MAP_VALUE_BYTEBUFFER_WITH_EVENT_TIME
    };

    private enum EmitType
    {
        MAP, MAP_WITH_EVENT_TIME,
        MSGPACK_MAP_VALUE_BYTES, MSGPACK_MAP_VALUE_BYTES_WITH_EVENT_TIME,
        MSGPACK_MAP_VALUE_BYTEBUFFER, MSGPACK_MAP_VALUE_BYTEBUFFER_WITH_EVENT_TIME,
    }

    public static class Options
    {
        private final boolean failover;
        private final boolean fileBackup;
        private final boolean closeInsteadOfFlush;
        private final boolean ackResponse;
        private final boolean smallBuffer;
        private final boolean useSsl;
        private final EmitType emitType;

        Options(
                boolean failover,
                boolean fileBackup,
                boolean closeInsteadOfFlush,
                boolean ackResponse,
                boolean smallBuffer,
                boolean useSsl)
        {
            this(failover, fileBackup, closeInsteadOfFlush, ackResponse, smallBuffer, useSsl, EmitType.MAP);
        }

        Options(
                boolean failover,
                boolean fileBackup,
                boolean closeInsteadOfFlush,
                boolean ackResponse,
                boolean smallBuffer,
                boolean useSsl,
                EmitType emitType)
        {
            this.failover = failover;
            this.fileBackup = fileBackup;
            this.closeInsteadOfFlush = closeInsteadOfFlush;
            this.ackResponse = ackResponse;
            this.smallBuffer = smallBuffer;
            this.useSsl = useSsl;
            this.emitType = emitType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Options)) {
                return false;
            }

            Options options = (Options) o;

            if (failover != options.failover) {
                return false;
            }
            if (fileBackup != options.fileBackup) {
                return false;
            }
            if (closeInsteadOfFlush != options.closeInsteadOfFlush) {
                return false;
            }
            if (ackResponse != options.ackResponse) {
                return false;
            }
            if (smallBuffer != options.smallBuffer) {
                return false;
            }
            if (useSsl != options.useSsl) {
                return false;
            }
            return emitType == options.emitType;
        }

        @Override
        public int hashCode()
        {
            int result = (failover ? 1 : 0);
            result = 31 * result + (fileBackup ? 1 : 0);
            result = 31 * result + (closeInsteadOfFlush ? 1 : 0);
            result = 31 * result + (ackResponse ? 1 : 0);
            result = 31 * result + (smallBuffer ? 1 : 0);
            result = 31 * result + (useSsl ? 1 : 0);
            result = 31 * result + (emitType != null ? emitType.hashCode() : 0);
            return result;
        }

        @Override
        public String toString()
        {
            return "Options{" +
                    "failover=" + failover +
                    ", fileBackup=" + fileBackup +
                    ", closeInsteadOfFlush=" + closeInsteadOfFlush +
                    ", ackResponse=" + ackResponse +
                    ", smallBuffer=" + smallBuffer +
                    ", useSsl=" + useSsl +
                    ", emitType=" + emitType +
                    '}';
        }
    }

    interface FluencyFactory
    {
        Fluency generate(List<Integer> localPort)
                throws IOException;
    }

    private Sender getSingleTCPSender(int port)
    {
        return new TCPSender.Config().setPort(port).createInstance();
    }

    private Sender getDoubleTCPSender(int firstPort, int secondPort)
    {
        return new MultiSender.Config(
                Arrays.<Sender.Instantiator>asList(
                    new TCPSender.Config()
                            .setPort(firstPort)
                            .setHeartbeaterConfig(new TCPHeartbeater.Config().setPort(firstPort)),
                    new TCPSender.Config()
                            .setPort(secondPort)
                            .setHeartbeaterConfig(new TCPHeartbeater.Config().setPort(secondPort))
                )).createInstance();
    }

    private Sender getSingleSSLSender(int port)
    {
        return new SSLSender.Config().setPort(port).createInstance();
    }

    private Sender getDoubleSSLSender(int firstPort, int secondPort)
    {
        return new MultiSender.Config(
                Arrays.<Sender.Instantiator>asList(
                    new SSLSender.Config()
                            .setPort(firstPort)
                            .setHeartbeaterConfig(new TCPHeartbeater.Config().setPort(firstPort)),
                    new SSLSender.Config()
                            .setPort(secondPort)
                            .setHeartbeaterConfig(new TCPHeartbeater.Config().setPort(secondPort))
                )).createInstance();
    }

    @Theory
    public void testFluencyUsingAsyncFlusher(final Options options)
            throws Exception
    {
        testFluencyBase(new FluencyFactory()
        {
            @Override
            public Fluency generate(List<Integer> localPorts)
                    throws IOException
            {
                Sender sender;
                int fluentdPort = localPorts.get(0);
                if (options.failover) {
                    int secondaryFluentdPort = localPorts.get(1);
                    if (options.useSsl) {
                        sender = getDoubleSSLSender(fluentdPort, secondaryFluentdPort);
                    }
                    else {
                        sender = getDoubleTCPSender(fluentdPort, secondaryFluentdPort);
                    }
                }
                else {
                    if (options.useSsl) {
                        sender = getSingleSSLSender(fluentdPort);
                    }
                    else {
                        sender = getSingleTCPSender(fluentdPort);
                    }
                }
                PackedForwardBuffer.Config bufferConfig = new PackedForwardBuffer.Config();
                if (options.ackResponse) {
                    bufferConfig.setAckResponseMode(true);
                }
                if (options.smallBuffer) {
                    bufferConfig.setMaxBufferSize(SMALL_BUF_SIZE);
                }
                if (options.fileBackup) {
                    bufferConfig.setFileBackupDir(TMPDIR).setFileBackupPrefix("testFluencyUsingAsyncFlusher" + options.hashCode());
                }
                Flusher.Instantiator flusherConfig = new AsyncFlusher.Config()
                        .setWaitUntilBufferFlushed(10)
                        .setWaitUntilTerminated(10);
                return new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            }
        }, options);
    }

    private void testFluencyBase(final FluencyFactory fluencyFactory, final Options options)
            throws Exception
    {
        LOG.info("testFluencyBase starts: options={}", options);

        final ArrayList<Integer> localPorts = new ArrayList<Integer>();

        final MockFluentdServer fluentd = new MockFluentdServer(options.useSsl);
        fluentd.start();

        final MockFluentdServer secondaryFluentd = new MockFluentdServer(options.useSsl, fluentd);
        secondaryFluentd.start();

        localPorts.add(fluentd.getLocalPort());
        localPorts.add(secondaryFluentd.getLocalPort());

        final AtomicReference<Fluency> fluency = new AtomicReference<Fluency>(fluencyFactory.generate(localPorts));
        if (options.fileBackup) {
            fluency.get().clearBackupFiles();
        }

        final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

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
            final AtomicBoolean shouldFailOver = new AtomicBoolean(true);

            final AtomicBoolean shouldStopFluentd = new AtomicBoolean(true);
            final AtomicBoolean shouldStopFluency = new AtomicBoolean(true);
            final CountDownLatch fluentdCloseWaitLatch = new CountDownLatch(concurrency);
            final CountDownLatch fluencyCloseWaitLatch = new CountDownLatch(concurrency);

            ExecutorService es = Executors.newCachedThreadPool();
            for (int i = 0; i < concurrency; i++) {
                es.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        for (int i = 0; i < reqNum; i++) {
                            if (Thread.currentThread().isInterrupted()) {
                                LOG.info("Interrupted...");
                                break;
                            }

                            if (options.failover) {
                                if (i == reqNum / 2) {
                                    if (shouldFailOver.getAndSet(false)) {
                                        LOG.info("Failing over...");
                                        try {
                                            secondaryFluentd.stop();
                                        }
                                        catch (IOException e) {
                                            LOG.warn("Failed to stop secondary fluentd", e);
                                        }
                                    }
                                }
                            }
                            else if (options.fileBackup) {
                                if (i == reqNum / 2) {
                                    if (shouldStopFluentd.getAndSet(false)) {
                                        LOG.info("Stopping Fluentd...");
                                        try {
                                            fluentd.stop();
                                            secondaryFluentd.stop();
                                        }
                                        catch (IOException e) {
                                            LOG.warn("Failed to stop Fluentd", e);
                                        }
                                    }

                                    fluentdCloseWaitLatch.countDown();
                                    try {
                                        assertTrue(fluentdCloseWaitLatch.await(20, TimeUnit.SECONDS));
                                    }
                                    catch (InterruptedException e) {
                                        LOG.warn("Interrupted", e);
                                    }

                                    if (shouldStopFluency.getAndSet(false)) {
                                        LOG.info("Stopping Fluency...");
                                        try {
                                            fluency.get().close();
                                            TimeUnit.SECONDS.sleep(2);
                                        }
                                        catch (Exception e) {
                                            LOG.warn("Failed to stop Fluency", e);
                                        }

                                        LOG.info("Restarting Fluentd...");
                                        try {
                                            fluentd.start();
                                            secondaryFluentd.start();
                                            LOG.info("Restarting Fluency...");
                                            fluency.set(fluencyFactory.generate(Arrays.asList(fluentd.getLocalPort(), secondaryFluentd.getLocalPort())));
                                            TimeUnit.SECONDS.sleep(2);
                                        }
                                        catch (Exception e) {
                                            LOG.warn("Failed to restart Fluentd", e);
                                        }
                                    }

                                    fluencyCloseWaitLatch.countDown();
                                    try {
                                        assertTrue(fluencyCloseWaitLatch.await(20, TimeUnit.SECONDS));
                                    }
                                    catch (InterruptedException e) {
                                        LOG.warn("Interrupted", e);
                                    }
                                }
                            }
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
                                Exception exception = null;
                                for (int retry = 0; retry < 10; retry++) {
                                    try {
                                        switch (options.emitType) {
                                            case MAP: {
                                                fluency.get().emit(tag, hashMap);
                                                break;
                                            }
                                            case MAP_WITH_EVENT_TIME: {
                                                EventTime eventTime = EventTime.fromEpochMilli(System.currentTimeMillis());
                                                fluency.get().emit(tag, eventTime, hashMap);
                                                break;
                                            }
                                            case MSGPACK_MAP_VALUE_BYTES: {
                                                byte[] bytes = objectMapper.writeValueAsBytes(hashMap);
                                                fluency.get().emit(tag, bytes, 0, bytes.length);
                                                break;
                                            }
                                            case MSGPACK_MAP_VALUE_BYTES_WITH_EVENT_TIME: {
                                                EventTime eventTime = EventTime.fromEpochMilli(System.currentTimeMillis());
                                                byte[] bytes = objectMapper.writeValueAsBytes(hashMap);
                                                fluency.get().emit(tag, eventTime, bytes, 0, bytes.length);
                                                break;
                                            }
                                            case MSGPACK_MAP_VALUE_BYTEBUFFER: {
                                                byte[] bytes = objectMapper.writeValueAsBytes(hashMap);
                                                fluency.get().emit(tag, ByteBuffer.wrap(bytes));
                                                break;
                                            }
                                            case MSGPACK_MAP_VALUE_BYTEBUFFER_WITH_EVENT_TIME: {
                                                EventTime eventTime = EventTime.fromEpochMilli(System.currentTimeMillis());
                                                byte[] bytes = objectMapper.writeValueAsBytes(hashMap);
                                                fluency.get().emit(tag, eventTime, ByteBuffer.wrap(bytes));
                                                break;
                                            }
                                        }
                                        exception = null;
                                        break;
                                    }
                                    catch (Exception e) {
                                        exception = e;
                                        try {
                                            TimeUnit.SECONDS.sleep(1);
                                        }
                                        catch (InterruptedException e1) {
                                        }
                                    }
                                }
                                if (exception != null) {
                                    throw exception;
                                }
                            }
                            catch (Exception e) {
                                LOG.warn("Exception occurred", e);
                            }
                        }
                        latch.countDown();
                    }
                });
            }

            if (!latch.await(60, TimeUnit.SECONDS)) {
                assertTrue("Sending all requests is timed out", false);
            }

            if (options.closeInsteadOfFlush) {
                fluency.get().close();
            }
            else {
                fluency.get().flush();
                fluency.get().waitUntilAllBufferFlushed(20);
            }

            fluentd.waitUntilEventsStop();
            fluentd.stop();

            secondaryFluentd.waitUntilEventsStop();
            secondaryFluentd.stop();

            if (options.failover) {
                assertThat(fluentd.connectCounter.get(), is(greaterThan(0L)));
                assertThat(fluentd.connectCounter.get(), is(lessThanOrEqualTo(10L)));
                assertThat(fluentd.closeCounter.get(), is(greaterThan(0L)));
                assertThat(fluentd.closeCounter.get(), is(lessThanOrEqualTo(10L)));
            }
            else {
                assertThat(fluentd.connectCounter.get(), is(greaterThan(0L)));
                assertThat(fluentd.connectCounter.get(), is(lessThanOrEqualTo(2L)));
                if (options.closeInsteadOfFlush) {
                    assertThat(fluentd.closeCounter.get(), is(greaterThan(0L)));
                }
                else {
                    assertThat(fluentd.closeCounter.get(), is(0L));
                }
                assertThat(fluentd.closeCounter.get(), is(lessThanOrEqualTo(2L)));
            }

            assertEquals((long) concurrency * reqNum, fluentd.ageEventsCounter.get());
            assertEquals(ageEventsSum.get(), fluentd.ageEventsSum.get());
            assertEquals((long) concurrency * reqNum, fluentd.nameEventsCounter.get());
            assertEquals(nameEventsLength.get(), fluentd.nameEventsLength.get());
            assertEquals(tag0EventsCounter.get(), fluentd.tag0EventsCounter.get());
            assertEquals(tag1EventsCounter.get(), fluentd.tag1EventsCounter.get());
            assertEquals(tag2EventsCounter.get(), fluentd.tag2EventsCounter.get());
            assertEquals(tag3EventsCounter.get(), fluentd.tag3EventsCounter.get());

            System.out.println(System.currentTimeMillis() - start);
        }
        finally {
            fluency.get().close();
            fluentd.stop();
            secondaryFluentd.stop();
        }
    }

    private static class MockFluentdServer
            extends AbstractFluentdServer
    {
        private final AtomicLong connectCounter;
        private final AtomicLong ageEventsCounter;
        private final AtomicLong ageEventsSum;
        private final AtomicLong nameEventsCounter;
        private final AtomicLong nameEventsLength;
        private final AtomicLong tag0EventsCounter;
        private final AtomicLong tag1EventsCounter;
        private final AtomicLong tag2EventsCounter;
        private final AtomicLong tag3EventsCounter;
        private final AtomicLong closeCounter;
        private final long startTimestampMillis;

        public MockFluentdServer(boolean useSsl)
                throws Exception
        {
            super(useSsl);
            connectCounter = new AtomicLong();
            ageEventsCounter = new AtomicLong();
            ageEventsSum = new AtomicLong();
            nameEventsCounter = new AtomicLong();
            nameEventsLength = new AtomicLong();
            tag0EventsCounter = new AtomicLong();
            tag1EventsCounter = new AtomicLong();
            tag2EventsCounter = new AtomicLong();
            tag3EventsCounter = new AtomicLong();
            closeCounter = new AtomicLong();
            startTimestampMillis = System.currentTimeMillis();
        }

        public MockFluentdServer(boolean useSsl, MockFluentdServer base)
                throws Exception
        {
            super(useSsl);
            connectCounter = base.connectCounter;
            ageEventsCounter = base.ageEventsCounter;
            ageEventsSum = base.ageEventsSum;
            nameEventsCounter = base.nameEventsCounter;
            nameEventsLength = base.nameEventsLength;
            tag0EventsCounter = base.tag0EventsCounter;
            tag1EventsCounter = base.tag1EventsCounter;
            tag2EventsCounter = base.tag2EventsCounter;
            tag3EventsCounter = base.tag3EventsCounter;
            closeCounter = base.closeCounter;
            startTimestampMillis = System.currentTimeMillis();
        }

        @Override
        protected EventHandler getFluentdEventHandler()
        {
            return new EventHandler()
            {
                @Override
                public void onConnect(Socket acceptSocket)
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
                    assertTrue(startTimestampMillis / 1000 <= timestampMillis / 1000 && timestampMillis < startTimestampMillis + 60 * 1000);

                    assertEquals(4, data.size());
                    for (Map.Entry<Value, Value> kv : data.entrySet()) {
                        String key = kv.getKey().asStringValue().toString();
                        Value val = kv.getValue();
                        if (key.equals("comment")) {
                            assertEquals("hello, world", val.toString());
                        }
                        else if (key.equals("rate")) {
                            assertEquals(1.23, val.asFloatValue().toFloat(), 0.000001);
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
                public void onClose(Socket accpetSocket)
                {
                    closeCounter.incrementAndGet();
                }
            };
        }
    }
}
