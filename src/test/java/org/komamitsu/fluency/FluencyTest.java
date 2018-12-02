/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.buffer.TestableBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.flusher.SyncFlusher;
import org.komamitsu.fluency.sender.MockTCPSender;
import org.komamitsu.fluency.sender.MultiSender;
import org.komamitsu.fluency.sender.NetworkSender;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.SSLSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.SenderErrorHandler;
import org.komamitsu.fluency.sender.TCPSender;
import org.komamitsu.fluency.sender.fluentd.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.fluentd.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.sender.fluentd.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.sender.fluentd.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.sender.fluentd.retry.ExponentialBackOffRetryStrategy;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableRawValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Theories.class)
public class FluencyTest
{
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTest.class);
    private static final StringValue KEY_OPTION_SIZE = ValueFactory.newString("size");
    private static final StringValue KEY_OPTION_CHUNK = ValueFactory.newString("chunk");

    @DataPoints
    public static final boolean[] SSL_ENABLED = { false, true };

    private void assertDefaultBuffer(Buffer buffer)
    {
        assertThat(buffer, instanceOf(PackedForwardBuffer.class));
        PackedForwardBuffer packedForwardBuffer = (PackedForwardBuffer) buffer;
        assertThat(packedForwardBuffer.getMaxBufferSize(), is(512 * 1024 * 1024L));
        assertThat(packedForwardBuffer.getFileBackupDir(), is(nullValue()));
        assertThat(packedForwardBuffer.bufferFormatType(), is("packed_forward"));
        assertThat(packedForwardBuffer.getChunkExpandRatio(), is(2f));
        assertThat(packedForwardBuffer.getChunkRetentionSize(), is(4 * 1024 * 1024));
        assertThat(packedForwardBuffer.getChunkInitialSize(), is(1 * 1024 * 1024));
        assertThat(packedForwardBuffer.getChunkRetentionTimeMillis(), is(1000));
        assertThat(packedForwardBuffer.getJvmHeapBufferMode(), is(false));
        assertThat(packedForwardBuffer.isAckResponseMode(), is(false));
    }

    private void assertDefaultFlusher(Flusher flusher)
    {
        assertThat(flusher, instanceOf(AsyncFlusher.class));
        AsyncFlusher asyncFlusher = (AsyncFlusher) flusher;
        assertThat(asyncFlusher.isTerminated(), is(false));
        assertThat(asyncFlusher.getFlushIntervalMillis(), is(600));
        assertThat(asyncFlusher.getWaitUntilBufferFlushed(), is(60));
        assertThat(asyncFlusher.getWaitUntilTerminated(), is(60));
    }

    private void assertDefaultRetryableSender(RetryableSender sender, Class<? extends NetworkSender> expectedBaseClass)
    {
        assertThat(sender.getRetryStrategy(), instanceOf(ExponentialBackOffRetryStrategy.class));
        ExponentialBackOffRetryStrategy retryStrategy = (ExponentialBackOffRetryStrategy) sender.getRetryStrategy();
        assertThat(retryStrategy.getMaxRetryCount(), is(7));
        assertThat(retryStrategy.getBaseIntervalMillis(), is(400));
        assertThat(sender.getBaseSender(), instanceOf(expectedBaseClass));
    }

    private void assertDefaultSender(Sender sender, String expectedHost, int expectedPort, Class<? extends NetworkSender> expectedBaseClass)
    {
        assertThat(sender, instanceOf(RetryableSender.class));
        RetryableSender retryableSender = (RetryableSender) sender;
        assertDefaultRetryableSender(retryableSender, expectedBaseClass);

        NetworkSender networkSender = (NetworkSender) retryableSender.getBaseSender();
        assertThat(networkSender.getHost(), is(expectedHost));
        assertThat(networkSender.getPort(), is(expectedPort));
        assertThat(networkSender.getConnectionTimeoutMilli(), is(5000));
        assertThat(networkSender.getReadTimeoutMilli(), is(5000));

        FailureDetector failureDetector = networkSender.getFailureDetector();
        assertThat(failureDetector, is(nullValue()));
    }

    @Test
    public void testDefaultFluency()
            throws IOException
    {
        Fluency fluency = null;
        try {
            fluency = Fluency.defaultFluency();
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultSender(fluency.getFlusher().getSender(), "127.0.0.1", 24224, TCPSender.class);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithCustomPort()
            throws IOException
    {
        Fluency fluency = null;
        try {
            fluency = Fluency.defaultFluency(54321);
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultSender(fluency.getFlusher().getSender(), "127.0.0.1", 54321, TCPSender.class);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithCustomHostAndPort()
            throws IOException
    {
        Fluency fluency = null;
        try {
            fluency = Fluency.defaultFluency("192.168.0.99", 54321);
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultSender(fluency.getFlusher().getSender(), "192.168.0.99", 54321, TCPSender.class);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithSsl()
            throws IOException
    {
        Fluency fluency = null;
        try {
            fluency = Fluency.defaultFluency(new FluencyConfig().setSslEnabled(true));
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultSender(fluency.getFlusher().getSender(), "127.0.0.1", 24224, SSLSender.class);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithSslAndCustomPort()
            throws IOException
    {
        Fluency fluency = null;
        try {
            fluency = Fluency.defaultFluency(54321, new FluencyConfig().setSslEnabled(true));
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultSender(fluency.getFlusher().getSender(), "127.0.0.1", 54321, SSLSender.class);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithSslAndCustomHostAndPort()
            throws IOException
    {
        Fluency fluency = null;
        try {
            fluency = Fluency.defaultFluency("192.168.0.99", 54321, new FluencyConfig().setSslEnabled(true));
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultSender(fluency.getFlusher().getSender(), "192.168.0.99", 54321, SSLSender.class);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithComplexConfig()
            throws IOException
    {
        Fluency fluency = null;
        try {
            String tmpdir = System.getProperty("java.io.tmpdir");
            assertThat(tmpdir, is(notNullValue()));

            FluencyConfig config =
                    new FluencyConfig()
                            .setFlushIntervalMillis(200)
                            .setMaxBufferSize(Long.MAX_VALUE)
                            .setBufferChunkInitialSize(7 * 1024 * 1024)
                            .setBufferChunkRetentionSize(13 * 1024 * 1024)
                            .setJvmHeapBufferMode(true)
                            .setSenderMaxRetryCount(99)
                            .setAckResponseMode(true)
                            .setWaitUntilBufferFlushed(42)
                            .setWaitUntilFlusherTerminated(24)
                            .setFileBackupDir(tmpdir);

            fluency = Fluency.defaultFluency(
                    Arrays.asList(
                            new InetSocketAddress("333.333.333.333", 11111),
                            new InetSocketAddress("444.444.444.444", 22222)), config);

            assertThat(fluency.getBuffer(), instanceOf(PackedForwardBuffer.class));
            PackedForwardBuffer buffer = (PackedForwardBuffer) fluency.getBuffer();
            assertThat(buffer.getMaxBufferSize(), is(Long.MAX_VALUE));
            assertThat(buffer.getFileBackupDir(), is(tmpdir));
            assertThat(buffer.bufferFormatType(), is("packed_forward"));
            assertThat(buffer.getChunkRetentionTimeMillis(), is(1000));
            assertThat(buffer.getChunkExpandRatio(), is(2f));
            assertThat(buffer.getChunkInitialSize(), is(7 * 1024 * 1024));
            assertThat(buffer.getChunkRetentionSize(), is(13 * 1024 * 1024));
            assertThat(buffer.getJvmHeapBufferMode(), is(true));
            assertThat(buffer.isAckResponseMode(), is(true));

            assertThat(fluency.getFlusher(), instanceOf(AsyncFlusher.class));
            AsyncFlusher flusher = (AsyncFlusher) fluency.getFlusher();
            assertThat(flusher.isTerminated(), is(false));
            assertThat(flusher.getFlushIntervalMillis(), is(200));
            assertThat(flusher.getWaitUntilBufferFlushed(), is(42));
            assertThat(flusher.getWaitUntilTerminated(), is(24));

            assertThat(flusher.getSender(), instanceOf(RetryableSender.class));
            RetryableSender retryableSender = (RetryableSender) flusher.getSender();
            assertThat(retryableSender.getRetryStrategy(), instanceOf(ExponentialBackOffRetryStrategy.class));
            ExponentialBackOffRetryStrategy retryStrategy = (ExponentialBackOffRetryStrategy) retryableSender.getRetryStrategy();
            assertThat(retryStrategy.getMaxRetryCount(), is(99));
            assertThat(retryStrategy.getBaseIntervalMillis(), is(400));

            assertThat(retryableSender.getBaseSender(), instanceOf(MultiSender.class));
            MultiSender multiSender = (MultiSender) retryableSender.getBaseSender();
            assertThat(multiSender.getSenders().size(), is(2));

            assertThat(multiSender.getSenders().get(0), instanceOf(TCPSender.class));
            {
                TCPSender sender = (TCPSender) multiSender.getSenders().get(0);
                assertThat(sender.getHost(), is("333.333.333.333"));
                assertThat(sender.getPort(), is(11111));
                assertThat(sender.getConnectionTimeoutMilli(), is(5000));
                assertThat(sender.getReadTimeoutMilli(), is(5000));

                FailureDetector failureDetector = sender.getFailureDetector();
                assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
                assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
                assertThat(failureDetector.getHeartbeater(), instanceOf(TCPHeartbeater.class));
                assertThat(failureDetector.getHeartbeater().getHost(), is("333.333.333.333"));
                assertThat(failureDetector.getHeartbeater().getPort(), is(11111));
                assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
            }

            assertThat(multiSender.getSenders().get(1), instanceOf(TCPSender.class));
            {
                TCPSender sender = (TCPSender) multiSender.getSenders().get(1);
                assertThat(sender.getHost(), is("444.444.444.444"));
                assertThat(sender.getPort(), is(22222));
                assertThat(sender.getConnectionTimeoutMilli(), is(5000));
                assertThat(sender.getReadTimeoutMilli(), is(5000));

                FailureDetector failureDetector = sender.getFailureDetector();
                assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
                assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
                assertThat(failureDetector.getHeartbeater(), instanceOf(TCPHeartbeater.class));
                assertThat(failureDetector.getHeartbeater().getHost(), is("444.444.444.444"));
                assertThat(failureDetector.getHeartbeater().getPort(), is(22222));
                assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
            }
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testDefaultFluencyWithSslAndComplexConfig()
            throws IOException
    {
        Fluency fluency = null;
        try {
            String tmpdir = System.getProperty("java.io.tmpdir");
            assertThat(tmpdir, is(notNullValue()));

            FluencyConfig config =
                    new FluencyConfig()
                            .setSslEnabled(true)
                            .setFlushIntervalMillis(200)
                            .setMaxBufferSize(Long.MAX_VALUE)
                            .setBufferChunkInitialSize(7 * 1024 * 1024)
                            .setBufferChunkRetentionSize(13 * 1024 * 1024)
                            .setJvmHeapBufferMode(true)
                            .setSenderMaxRetryCount(99)
                            .setAckResponseMode(true)
                            .setWaitUntilBufferFlushed(42)
                            .setWaitUntilFlusherTerminated(24)
                            .setFileBackupDir(tmpdir);

            fluency = Fluency.defaultFluency(
                    Arrays.asList(
                            new InetSocketAddress("333.333.333.333", 11111),
                            new InetSocketAddress("444.444.444.444", 22222)), config);

            assertThat(fluency.getFlusher().getSender(), instanceOf(RetryableSender.class));
            RetryableSender retryableSender = (RetryableSender) fluency.getFlusher().getSender();
            assertThat(retryableSender.getRetryStrategy(), instanceOf(ExponentialBackOffRetryStrategy.class));
            ExponentialBackOffRetryStrategy retryStrategy = (ExponentialBackOffRetryStrategy) retryableSender.getRetryStrategy();
            assertThat(retryStrategy.getMaxRetryCount(), is(99));
            assertThat(retryStrategy.getBaseIntervalMillis(), is(400));

            assertThat(retryableSender.getBaseSender(), instanceOf(MultiSender.class));
            MultiSender multiSender = (MultiSender) retryableSender.getBaseSender();
            assertThat(multiSender.getSenders().size(), is(2));

            assertThat(multiSender.getSenders().get(0), instanceOf(SSLSender.class));
            {
                SSLSender sender = (SSLSender) multiSender.getSenders().get(0);
                assertThat(sender.getHost(), is("333.333.333.333"));
                assertThat(sender.getPort(), is(11111));
                assertThat(sender.getConnectionTimeoutMilli(), is(5000));
                assertThat(sender.getReadTimeoutMilli(), is(5000));

                FailureDetector failureDetector = sender.getFailureDetector();
                assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
                assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
                assertThat(failureDetector.getHeartbeater(), instanceOf(SSLHeartbeater.class));
                assertThat(failureDetector.getHeartbeater().getHost(), is("333.333.333.333"));
                assertThat(failureDetector.getHeartbeater().getPort(), is(11111));
                assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
            }

            assertThat(multiSender.getSenders().get(1), instanceOf(SSLSender.class));
            {
                SSLSender sender = (SSLSender) multiSender.getSenders().get(1);
                assertThat(sender.getHost(), is("444.444.444.444"));
                assertThat(sender.getPort(), is(22222));
                assertThat(sender.getConnectionTimeoutMilli(), is(5000));
                assertThat(sender.getReadTimeoutMilli(), is(5000));

                FailureDetector failureDetector = sender.getFailureDetector();
                assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
                assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
                assertThat(failureDetector.getHeartbeater(), instanceOf(SSLHeartbeater.class));
                assertThat(failureDetector.getHeartbeater().getHost(), is("444.444.444.444"));
                assertThat(failureDetector.getHeartbeater().getPort(), is(22222));
                assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
            }
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void testIsTerminated()
            throws IOException, InterruptedException
    {
        Sender sender = new MockTCPSender(24224);
        TestableBuffer.Config bufferConfig = new TestableBuffer.Config();
        {
            Flusher.Instantiator flusherConfig = new AsyncFlusher.Config();
            Fluency fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            assertFalse(fluency.isTerminated());
            fluency.close();
            TimeUnit.SECONDS.sleep(1);
            assertTrue(fluency.isTerminated());
        }

        {
            Flusher.Instantiator flusherConfig = new SyncFlusher.Config();
            Fluency fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            assertFalse(fluency.isTerminated());
            fluency.close();
            TimeUnit.SECONDS.sleep(1);
            assertTrue(fluency.isTerminated());
        }
    }

    @Test
    public void testGetAllocatedBufferSize()
            throws IOException
    {
        Fluency fluency = new Fluency.Builder(new MockTCPSender(24224)).
                setBufferConfig(new TestableBuffer.Config()).
                setFlusherConfig(new AsyncFlusher.Config()).
                build();
        assertThat(fluency.getAllocatedBufferSize(), is(0L));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("comment", "hello world");
        for (int i = 0; i < 10000; i++) {
            fluency.emit("foodb.bartbl", map);
        }
        assertThat(fluency.getAllocatedBufferSize(), is(TestableBuffer.ALLOC_SIZE * 10000L));
    }

    @Test
    public void testWaitUntilFlusherTerminated()
            throws IOException, InterruptedException
    {
        {
            Sender sender = new MockTCPSender(24224);
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config().setWaitBeforeCloseMillis(2000);
            AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config().setWaitUntilTerminated(0);
            Fluency fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            fluency.emit("foo.bar", new HashMap<String, Object>());
            fluency.close();
            assertThat(fluency.waitUntilFlusherTerminated(1), is(false));
        }

        {
            Sender sender = new MockTCPSender(24224);
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config().setWaitBeforeCloseMillis(2000);
            AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config().setWaitUntilTerminated(0);
            Fluency fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
            fluency.emit("foo.bar", new HashMap<String, Object>());
            fluency.close();
            assertThat(fluency.waitUntilFlusherTerminated(3), is(true));
        }
    }

    @Test
    public void testWaitUntilFlushingAllBuffer()
            throws IOException, InterruptedException
    {
        {
            Sender sender = new MockTCPSender(24224);
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config();
            Flusher.Instantiator flusherConfig = new AsyncFlusher.Config().setFlushIntervalMillis(2000);
            Fluency fluency = null;
            try {
                fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
                fluency.emit("foo.bar", new HashMap<String, Object>());
                assertThat(fluency.waitUntilAllBufferFlushed(3), is(true));
            }
            finally {
                if (fluency != null) {
                    fluency.close();
                }
            }
        }

        {
            Sender sender = new MockTCPSender(24224);
            TestableBuffer.Config bufferConfig = new TestableBuffer.Config();
            Flusher.Instantiator flusherConfig = new AsyncFlusher.Config().setFlushIntervalMillis(2000);
            Fluency fluency = null;
            try {
                fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).setFlusherConfig(flusherConfig).build();
                fluency.emit("foo.bar", new HashMap<String, Object>());
                assertThat(fluency.waitUntilAllBufferFlushed(1), is(false));
            }
            finally {
                if (fluency != null) {
                    fluency.close();
                }
            }
        }
    }

    @Test
    public void testSenderErrorHandler()
            throws IOException, InterruptedException
    {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> errorContainer = new AtomicReference<Throwable>();

        Fluency fluency = Fluency.defaultFluency(Integer.MAX_VALUE,
                new FluencyConfig()
                        .setSenderMaxRetryCount(1)
                        .setSenderErrorHandler(new SenderErrorHandler()
                        {
                            @Override
                            public void handle(Throwable e)
                            {
                                errorContainer.set(e);
                                countDownLatch.countDown();
                            }
                        }));

        HashMap<String, Object> event = new HashMap<String, Object>();
        event.put("name", "foo");
        fluency.emit("tag", event);

        if (!countDownLatch.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Timeout");
        }

        assertThat(errorContainer.get(), is(instanceOf(RetryableSender.RetryOverException.class)));
    }

    @Theory
    public void testWithoutAckResponse(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                new ConfigurableTestServer.WithClientSocket() {
                    @Override
                    public void run(Socket clientSocket)
                            throws Exception
                    {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                        assertEquals(3, unpacker.unpackArrayHeader());
                        assertEquals("foo.bar", unpacker.unpackString());
                        ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                        Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                        assertEquals(1, map.size());
                        assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                        unpacker.close();
                    }
                },
                new ConfigurableTestServer.WithServerPort()
                {
                    @Override
                    public void run(int serverPort)
                            throws Exception
                    {
                        Fluency fluency =
                                Fluency.defaultFluency(serverPort,
                                        new FluencyConfig().setSslEnabled(sslEnabled));
                        fluency.emit("foo.bar", new HashMap<String, Object>());
                        fluency.close();
                    }
                }, 5000);
        assertNull(exception);
    }

    @Theory
    public void testWithAckResponseButNotReceiveToken(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                new ConfigurableTestServer.WithClientSocket() {
                    @Override
                    public void run(Socket clientSocket)
                            throws Exception
                    {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                        assertEquals(3, unpacker.unpackArrayHeader());
                        assertEquals("foo.bar", unpacker.unpackString());
                        ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                        Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                        assertEquals(2, map.size());
                        assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                        assertNotNull(map.get(KEY_OPTION_CHUNK).asRawValue().asString());
                        unpacker.close();
                    }
                },
                new ConfigurableTestServer.WithServerPort()
                {
                    @Override
                    public void run(int serverPort)
                            throws Exception
                    {
                        Fluency fluency =
                                Fluency.defaultFluency(serverPort,
                                        new FluencyConfig().setSslEnabled(sslEnabled).setAckResponseMode(true));
                        fluency.emit("foo.bar", new HashMap<String, Object>());
                        fluency.close();
                    }
                }, 5000);
        assertEquals(exception.getClass(), TimeoutException.class);
    }

    @Theory
    public void testWithAckResponseButWrongReceiveToken(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                new ConfigurableTestServer.WithClientSocket() {
                    @Override
                    public void run(Socket clientSocket)
                            throws Exception
                    {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                        assertEquals(3, unpacker.unpackArrayHeader());
                        assertEquals("foo.bar", unpacker.unpackString());
                        ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                        Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                        assertEquals(2, map.size());
                        assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                        assertNotNull(map.get(KEY_OPTION_CHUNK).asRawValue().asString());

                        MessagePacker packer = MessagePack.newDefaultPacker(clientSocket.getOutputStream());
                        packer.packMapHeader(1)
                                .packString("ack").packString(UUID.randomUUID().toString())
                                .close();

                        // Close the input stream after closing the output stream to avoid closing a socket too early
                        unpacker.close();
                    }
                },
                new ConfigurableTestServer.WithServerPort()
                {
                    @Override
                    public void run(int serverPort)
                            throws Exception
                    {
                        Fluency fluency =
                                Fluency.defaultFluency(serverPort,
                                        new FluencyConfig().setSslEnabled(sslEnabled).setAckResponseMode(true));
                        fluency.emit("foo.bar", new HashMap<String, Object>());
                        fluency.close();
                    }
                }, 5000);
        assertEquals(exception.getClass(), TimeoutException.class);
    }

    @Theory
    public void testWithAckResponseWithProperToken(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                new ConfigurableTestServer.WithClientSocket() {
                    @Override
                    public void run(Socket clientSocket)
                            throws Exception
                    {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                        assertEquals(3, unpacker.unpackArrayHeader());
                        assertEquals("foo.bar", unpacker.unpackString());
                        ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                        Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                        assertEquals(2, map.size());
                        assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                        String ackResponseToken = map.get(KEY_OPTION_CHUNK).asRawValue().asString();
                        assertNotNull(ackResponseToken);

                        MessagePacker packer = MessagePack.newDefaultPacker(clientSocket.getOutputStream());
                        packer.packMapHeader(1)
                                .packString("ack").packString(ackResponseToken)
                                .close();

                        // Close the input stream after closing the output stream to avoid closing a socket too early
                        unpacker.close();
                    }
                },
                new ConfigurableTestServer.WithServerPort()
                {
                    @Override
                    public void run(int serverPort)
                            throws Exception
                    {
                        Fluency fluency = Fluency.defaultFluency(serverPort,
                                new FluencyConfig().setSslEnabled(sslEnabled).setAckResponseMode(true));
                        fluency.emit("foo.bar", new HashMap<String, Object>());
                        fluency.close();
                    }
                }, 5000);
        assertNull(exception);
    }

    private static class StuckSender
            extends StubSender
    {
        private final CountDownLatch latch;

        StuckSender(CountDownLatch latch)
        {
            this.latch = latch;
        }

        @Override
        public void send(ByteBuffer buffer)
                throws IOException
        {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                FluencyTest.LOG.warn("Interrupted in send()", e);
            }
        }

        @Override
        public boolean isAvailable()
        {
            return true;
        }
    }

    @Test
    public void testBufferFullException()
            throws IOException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        Sender stuckSender = new StuckSender(latch);

        try {
            PackedForwardBuffer.Config bufferConfig = new PackedForwardBuffer.Config().setChunkInitialSize(64).setMaxBufferSize(256);
            Fluency fluency = new Fluency.Builder(stuckSender).setBufferConfig(bufferConfig).build();
            Map<String, Object> event = new HashMap<String, Object>();
            event.put("name", "xxxx");
            for (int i = 0; i < 7; i++) {
                fluency.emit("tag", event);
            }
            try {
                fluency.emit("tag", event);
                assertTrue(false);
            }
            catch (BufferFullException e) {
                assertTrue(true);
            }
        }
        finally {
            latch.countDown();
        }
    }

    static class Foo
    {
        String s;
    }

    public static class FooSerializer
            extends StdSerializer<Foo>
    {
        final AtomicBoolean serialized;

        FooSerializer(AtomicBoolean serialized)
        {
            super(Foo.class);
            this.serialized = serialized;
        }

        @Override
        public void serialize(Foo value, JsonGenerator gen, SerializerProvider provider)
                throws IOException
        {
            gen.writeStartObject();
            gen.writeStringField("s", "Foo:" + value.s);
            gen.writeEndObject();
            serialized.set(true);
        }
    }

    @Test
    public void testBufferWithJacksonModule()
            throws IOException
    {
        AtomicBoolean serialized = new AtomicBoolean();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(Foo.class, new FooSerializer(serialized));

        PackedForwardBuffer.Config bufferConfig = new PackedForwardBuffer
                .Config()
                .setChunkInitialSize(64)
                .setMaxBufferSize(256)
                .setJacksonModules(Collections.<Module>singletonList(simpleModule));

        Fluency fluency = new Fluency.Builder(new TCPSender.Config()
                .createInstance())
                .setBufferConfig(bufferConfig)
                .build();

        Map<String, Object> event = new HashMap<String, Object>();
        Foo foo = new Foo();
        foo.s = "Hello";
        event.put("foo", foo);
        fluency.emit("tag", event);

        assertThat(serialized.get(), is(true));
    }
}
