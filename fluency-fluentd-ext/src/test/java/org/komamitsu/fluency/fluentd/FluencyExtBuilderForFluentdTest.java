package org.komamitsu.fluency.fluentd;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.ingester.sender.UnixSocketSender;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.UnixSocketHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.flusher.Flusher;

class FluencyExtBuilderForFluentdTest {
  // These assertMethods are copied from FluencyBuilderForFluentdTest
  private void assertBuffer(Buffer buffer) {
    assertThat(buffer.getMaxBufferSize()).isEqualTo(512 * 1024 * 1024L);
    assertThat(buffer.getFileBackupDir()).isNull();
    assertThat(buffer.bufferFormatType()).isEqualTo("packed_forward");
    assertThat(buffer.getChunkExpandRatio()).isEqualTo(2f);
    assertThat(buffer.getChunkRetentionSize()).isEqualTo(4 * 1024 * 1024);
    assertThat(buffer.getChunkInitialSize()).isEqualTo(1 * 1024 * 1024);
    assertThat(buffer.getChunkRetentionTimeMillis()).isEqualTo(1000);
    assertThat(buffer.getJvmHeapBufferMode()).isFalse();
  }

  private void assertFlusher(Flusher flusher) {
    assertThat(flusher.isTerminated()).isFalse();
    assertThat(flusher.getFlushAttemptIntervalMillis()).isEqualTo(600);
    assertThat(flusher.getWaitUntilBufferFlushed()).isEqualTo(60);
    assertThat(flusher.getWaitUntilTerminated()).isEqualTo(60);
  }

  private void assertDefaultRetryableSender(
      RetryableSender sender, Class<? extends UnixSocketSender> expectedBaseClass) {
    assertThat(sender.getRetryStrategy()).isInstanceOf(ExponentialBackOffRetryStrategy.class);
    ExponentialBackOffRetryStrategy retryStrategy =
        (ExponentialBackOffRetryStrategy) sender.getRetryStrategy();
    assertThat(retryStrategy.getMaxRetryCount()).isEqualTo(7);
    assertThat(retryStrategy.getBaseIntervalMillis()).isEqualTo(400);
    assertThat(sender.getBaseSender()).isInstanceOf(expectedBaseClass);
  }

  private void assertUnixSocketSender(
      UnixSocketSender sender, Path expectedPath, boolean shouldHaveFailureDetector) {
    assertThat(sender.getPath()).isEqualTo(expectedPath);
    assertThat(sender.getConnectionTimeoutMilli()).isEqualTo(5000);
    assertThat(sender.getReadTimeoutMilli()).isEqualTo(5000);

    FailureDetector failureDetector = sender.getFailureDetector();
    if (shouldHaveFailureDetector) {
      assertThat(failureDetector.getFailureIntervalMillis()).isEqualTo(3 * 1000);
      assertThat(failureDetector.getFailureDetectStrategy())
          .isInstanceOf(PhiAccrualFailureDetectStrategy.class);
      assertThat(failureDetector.getHeartbeater()).isInstanceOf(UnixSocketHeartbeater.class);
      {
        UnixSocketHeartbeater hb = (UnixSocketHeartbeater) failureDetector.getHeartbeater();
        assertThat(hb.getPath()).isEqualTo(expectedPath);
      }
      assertThat(failureDetector.getHeartbeater().getIntervalMillis()).isEqualTo(1000);
    } else {
      assertThat(failureDetector).isNull();
    }
  }

  private void assertDefaultFluentdSender(FluentdSender sender, Path expectedPath) {
    assertThat(sender).isInstanceOf(RetryableSender.class);
    RetryableSender retryableSender = (RetryableSender) sender;
    assertDefaultRetryableSender(retryableSender, UnixSocketSender.class);
    assertUnixSocketSender((UnixSocketSender) retryableSender.getBaseSender(), expectedPath, false);
  }

  @Test
  void build() throws IOException {
    Path socketPath = Paths.get(System.getProperty("java.io.tmpdir"), "foo/bar.socket");
    try (Fluency fluency = new FluencyExtBuilderForFluentd().build(socketPath)) {
      assertBuffer(fluency.getBuffer());
      assertFlusher(fluency.getFlusher());
      assertDefaultFluentdSender(
          (FluentdSender) fluency.getFlusher().getIngester().getSender(), socketPath);
    }
  }
}
