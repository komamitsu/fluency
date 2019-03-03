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

package org.komamitsu.fluency.fluentd;

import org.junit.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.MultiSender;
import org.komamitsu.fluency.fluentd.ingester.sender.NetworkSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.ingester.sender.SSLSender;
import org.komamitsu.fluency.fluentd.ingester.sender.TCPSender;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.flusher.Flusher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class FluencyBuilderForFluentdTest
{
    private void assertBuffer(Buffer buffer)
    {
        assertThat(buffer.getMaxBufferSize(), is(512 * 1024 * 1024L));
        assertThat(buffer.getFileBackupDir(), is(nullValue()));
        assertThat(buffer.bufferFormatType(), is("packed_forward"));
        assertThat(buffer.getChunkExpandRatio(), is(2f));
        assertThat(buffer.getChunkRetentionSize(), is(4 * 1024 * 1024));
        assertThat(buffer.getChunkInitialSize(), is(1 * 1024 * 1024));
        assertThat(buffer.getChunkRetentionTimeMillis(), is(1000));
        assertThat(buffer.getJvmHeapBufferMode(), is(false));
    }

    private void assertFlusher(Flusher flusher)
    {
        assertThat(flusher.isTerminated(), is(false));
        assertThat(flusher.getFlushIntervalMillis(), is(600));
        assertThat(flusher.getWaitUntilBufferFlushed(), is(60));
        assertThat(flusher.getWaitUntilTerminated(), is(60));
    }

    private void assertDefaultRetryableSender(RetryableSender sender, Class<? extends NetworkSender> expectedBaseClass)
    {
        assertThat(sender.getRetryStrategy(), instanceOf(ExponentialBackOffRetryStrategy.class));
        ExponentialBackOffRetryStrategy retryStrategy = (ExponentialBackOffRetryStrategy) sender.getRetryStrategy();
        assertThat(retryStrategy.getMaxRetryCount(), is(7));
        assertThat(retryStrategy.getBaseIntervalMillis(), is(400));
        assertThat(sender.getBaseSender(), instanceOf(expectedBaseClass));
    }

    private void assertDefaultFluentdSender(FluentdSender sender, String expectedHost, int expectedPort, Class<? extends NetworkSender> expectedBaseClass)
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
    public void build()
            throws IOException
    {
        try (Fluency fluency = new FluencyBuilderForFluentd().build()) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (FluentdSender) fluency.getFlusher().getIngester().getSender(),
                    "127.0.0.1",
                    24224,
                    TCPSender.class);
        }
    }

    @Test
    public void buildWithCustomPort()
            throws IOException
    {
        try (Fluency fluency = new FluencyBuilderForFluentd().build(54321)) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (FluentdSender) fluency.getFlusher().getIngester().getSender(),
                    "127.0.0.1",
                    54321,
                    TCPSender.class);
        }
    }

    @Test
    public void buildWithCustomHostAndPort()
            throws IOException
    {
        try (Fluency fluency = new FluencyBuilderForFluentd().build("192.168.0.99", 54321)) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (FluentdSender) fluency.getFlusher().getIngester().getSender(),
                    "192.168.0.99",
                    54321,
                    TCPSender.class);
        }
    }

    @Test
    public void buildWithSsl()
            throws IOException
    {
        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setSslEnabled(true);
        try (Fluency fluency = builder.build()) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (FluentdSender) fluency.getFlusher().getIngester().getSender(),
                    "127.0.0.1",
                    24224,
                    SSLSender.class);
        }
    }

    @Test
    public void buildWithSslAndCustomPort()
            throws IOException
    {
        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setSslEnabled(true);
        try (Fluency fluency = builder.build(54321)) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (FluentdSender) fluency.getFlusher().getIngester().getSender(),
                    "127.0.0.1",
                    54321,
                    SSLSender.class);
        }
    }

    @Test
    public void buildWithSslAndCustomHostAndPort()
            throws IOException
    {
        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setSslEnabled(true);
        try (Fluency fluency = builder.build("192.168.0.99", 54321)) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (FluentdSender) fluency.getFlusher().getIngester().getSender(),
                    "192.168.0.99",
                    54321,
                    SSLSender.class);
        }
    }

    @Test
    public void buildWithComplexConfig()
            throws IOException
    {
        String tmpdir = System.getProperty("java.io.tmpdir");
        assertThat(tmpdir, is(notNullValue()));

        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setFlushIntervalMillis(200);
        builder.setMaxBufferSize(Long.MAX_VALUE);
        builder.setBufferChunkInitialSize(7 * 1024 * 1024);
        builder.setBufferChunkRetentionSize(13 * 1024 * 1024);
        builder.setBufferChunkRetentionTimeMillis(19 * 1000);
        builder.setJvmHeapBufferMode(true);
        builder.setSenderMaxRetryCount(99);
        builder.setConnectionTimeoutMilli(12345);
        builder.setReadTimeoutMilli(9876);
        builder.setAckResponseMode(true);
        builder.setWaitUntilBufferFlushed(42);
        builder.setWaitUntilFlusherTerminated(24);
        builder.setFileBackupDir(tmpdir);

        try (Fluency fluency = builder.build(
                Arrays.asList(
                        new InetSocketAddress("333.333.333.333", 11111),
                        new InetSocketAddress("444.444.444.444", 22222)))) {

            assertThat(fluency.getBuffer(), instanceOf(Buffer.class));
            Buffer buffer = fluency.getBuffer();
            assertThat(buffer.getMaxBufferSize(), is(Long.MAX_VALUE));
            assertThat(buffer.getFileBackupDir(), is(tmpdir));
            assertThat(buffer.bufferFormatType(), is("packed_forward"));
            assertThat(buffer.getChunkRetentionTimeMillis(), is(19 * 1000));
            assertThat(buffer.getChunkExpandRatio(), is(2f));
            assertThat(buffer.getChunkInitialSize(), is(7 * 1024 * 1024));
            assertThat(buffer.getChunkRetentionSize(), is(13 * 1024 * 1024));
            assertThat(buffer.getJvmHeapBufferMode(), is(true));

            Flusher flusher = fluency.getFlusher();
            assertThat(flusher.isTerminated(), is(false));
            assertThat(flusher.getFlushIntervalMillis(), is(200));
            assertThat(flusher.getWaitUntilBufferFlushed(), is(42));
            assertThat(flusher.getWaitUntilTerminated(), is(24));

            assertThat(flusher.getIngester().getSender(), instanceOf(RetryableSender.class));
            RetryableSender retryableSender = (RetryableSender) flusher.getIngester().getSender();
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
                assertThat(sender.getConnectionTimeoutMilli(), is(12345));
                assertThat(sender.getReadTimeoutMilli(), is(9876));

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
                assertThat(sender.getConnectionTimeoutMilli(), is(12345));
                assertThat(sender.getReadTimeoutMilli(), is(9876));


                FailureDetector failureDetector = sender.getFailureDetector();
                assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
                assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
                assertThat(failureDetector.getHeartbeater(), instanceOf(TCPHeartbeater.class));
                assertThat(failureDetector.getHeartbeater().getHost(), is("444.444.444.444"));
                assertThat(failureDetector.getHeartbeater().getPort(), is(22222));
                assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
            }
        }
    }

    @Test
    public void buildWithSslAndComplexConfig()
            throws IOException
    {
        String tmpdir = System.getProperty("java.io.tmpdir");
        assertThat(tmpdir, is(notNullValue()));

        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setSslEnabled(true);
        builder.setFlushIntervalMillis(200);
        builder.setMaxBufferSize(Long.MAX_VALUE);
        builder.setBufferChunkInitialSize(7 * 1024 * 1024);
        builder.setBufferChunkRetentionSize(13 * 1024 * 1024);
        builder.setJvmHeapBufferMode(true);
        builder.setSenderMaxRetryCount(99);
        builder.setConnectionTimeoutMilli(12345);
        builder.setReadTimeoutMilli(9876);
        builder.setAckResponseMode(true);
        builder.setWaitUntilBufferFlushed(42);
        builder.setWaitUntilFlusherTerminated(24);
        builder.setFileBackupDir(tmpdir);

        try (Fluency fluency = builder.build(
                Arrays.asList(
                        new InetSocketAddress("333.333.333.333", 11111),
                        new InetSocketAddress("444.444.444.444", 22222)))) {

            assertThat(fluency.getFlusher().getIngester().getSender(), instanceOf(RetryableSender.class));
            RetryableSender retryableSender = (RetryableSender) fluency.getFlusher().getIngester().getSender();
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
                assertThat(sender.getConnectionTimeoutMilli(), is(12345));
                assertThat(sender.getReadTimeoutMilli(), is(9876));

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
                assertThat(sender.getConnectionTimeoutMilli(), is(12345));
                assertThat(sender.getReadTimeoutMilli(), is(9876));

                FailureDetector failureDetector = sender.getFailureDetector();
                assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
                assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
                assertThat(failureDetector.getHeartbeater(), instanceOf(SSLHeartbeater.class));
                assertThat(failureDetector.getHeartbeater().getHost(), is("444.444.444.444"));
                assertThat(failureDetector.getHeartbeater().getPort(), is(22222));
                assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
            }
        }
    }
}
