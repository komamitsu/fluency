package org.komamitsu.fluency.fluentd;

import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.MultiSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.ingester.sender.UnixSocketSender;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.UnixSocketHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.flusher.Flusher;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FluencyExtBuilderForFluentdTest
{
    // These assertMethods are copied from FluencyBuilderForFluentdTest
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
        assertThat(flusher.getFlushAttemptIntervalMillis(), is(600));
        assertThat(flusher.getWaitUntilBufferFlushed(), is(60));
        assertThat(flusher.getWaitUntilTerminated(), is(60));
    }

    private void assertDefaultRetryableSender(RetryableSender sender, Class<? extends UnixSocketSender> expectedBaseClass)
    {
        assertThat(sender.getRetryStrategy(), instanceOf(ExponentialBackOffRetryStrategy.class));
        ExponentialBackOffRetryStrategy retryStrategy = (ExponentialBackOffRetryStrategy) sender.getRetryStrategy();
        assertThat(retryStrategy.getMaxRetryCount(), is(7));
        assertThat(retryStrategy.getBaseIntervalMillis(), is(400));
        assertThat(sender.getBaseSender(), instanceOf(expectedBaseClass));
    }

    private void assertUnixSocketSender(UnixSocketSender sender, Path expectedPath, boolean shouldHaveFailureDetector)
    {
        assertThat(sender.getPath(), is(expectedPath));
        assertThat(sender.getConnectionTimeoutMilli(), is(5000));
        assertThat(sender.getReadTimeoutMilli(), is(5000));

        FailureDetector failureDetector = sender.getFailureDetector();
        if (shouldHaveFailureDetector) {
            assertThat(failureDetector.getFailureIntervalMillis(), is(3 * 1000));
            assertThat(failureDetector.getFailureDetectStrategy(), instanceOf(PhiAccrualFailureDetectStrategy.class));
            assertThat(failureDetector.getHeartbeater(), instanceOf(UnixSocketHeartbeater.class));
            {
                UnixSocketHeartbeater hb = (UnixSocketHeartbeater) failureDetector.getHeartbeater();
                assertThat(hb.getPath(), is(expectedPath));
            }
            assertThat(failureDetector.getHeartbeater().getIntervalMillis(), is(1000));
        }
        else {
            assertThat(failureDetector, is(nullValue()));
        }
    }

    private void assertDefaultFluentdSender(FluentdSender sender, Path expectedPath)
    {
        assertThat(sender, instanceOf(RetryableSender.class));
        RetryableSender retryableSender = (RetryableSender) sender;
        assertDefaultRetryableSender(retryableSender, UnixSocketSender.class);
        assertUnixSocketSender((UnixSocketSender) retryableSender.getBaseSender(), expectedPath, false);
    }

    @Test
    void build()
            throws IOException
    {
        Path socketPath = Paths.get(System.getProperty("java.io.tmpdir"), "foo/bar.socket");
        try (Fluency fluency = new FluencyExtBuilderForFluentd().build(socketPath)) {
            assertBuffer(fluency.getBuffer());
            assertFlusher(fluency.getFlusher());
            assertDefaultFluentdSender((FluentdSender) fluency.getFlusher().getIngester().getSender(), socketPath);
        }
    }
}
