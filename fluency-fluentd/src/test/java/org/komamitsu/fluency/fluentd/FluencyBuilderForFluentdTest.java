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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.fluentd.ingester.sender.*;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.fluentd.recordformat.FluentdRecordFormatter;
import org.komamitsu.fluency.flusher.Flusher;

class FluencyBuilderForFluentdTest {
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
      RetryableSender sender, Class<? extends NetworkSender> expectedBaseClass) {
    assertThat(sender.getRetryStrategy()).isInstanceOf(ExponentialBackOffRetryStrategy.class);
    ExponentialBackOffRetryStrategy retryStrategy =
        (ExponentialBackOffRetryStrategy) sender.getRetryStrategy();
    assertThat(retryStrategy.getMaxRetryCount()).isEqualTo(7);
    assertThat(retryStrategy.getBaseIntervalMillis()).isEqualTo(400);
    assertThat(sender.getBaseSender()).isInstanceOf(expectedBaseClass);
  }

  private void assertDefaultFluentdSender(
      FluentdSender sender,
      String expectedHost,
      int expectedPort,
      Class<? extends NetworkSender> expectedBaseClass) {
    assertThat(sender).isInstanceOf(RetryableSender.class);
    RetryableSender retryableSender = (RetryableSender) sender;
    assertDefaultRetryableSender(retryableSender, expectedBaseClass);

    assertThat(retryableSender.getBaseSender()).isInstanceOf(InetSocketSender.class);
    InetSocketSender<SocketChannel> networkSender =
        (InetSocketSender) retryableSender.getBaseSender();
    assertThat(networkSender.getHost()).isEqualTo(expectedHost);
    assertThat(networkSender.getPort()).isEqualTo(expectedPort);
    assertThat(networkSender.getConnectionTimeoutMilli()).isEqualTo(5000);
    assertThat(networkSender.getReadTimeoutMilli()).isEqualTo(5000);

    FailureDetector failureDetector = networkSender.getFailureDetector();
    assertThat(failureDetector).isNull();
  }

  @Test
  void build() throws IOException {
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
  void buildWithCustomPort() throws IOException {
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
  void buildWithCustomHostAndPort() throws IOException {
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
  void buildWithSsl() throws IOException {
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
  void buildWithSslAndCustomPort() throws IOException {
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
  void buildWithSslAndCustomHostAndPort() throws IOException {
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
  void buildWithSslSocketFactory() throws IOException {
    FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
    builder.setSslEnabled(true);
    SSLSocketFactory sslSocketFactory = mock(SSLSocketFactory.class);
    SSLSocket socket = mock(SSLSocket.class);
    RuntimeException expectedException = new RuntimeException("Expected");
    doThrow(expectedException).when(socket).connect(any(InetSocketAddress.class), anyInt());
    doNothing().when(socket).close();
    doReturn(socket).when(sslSocketFactory).createSocket();
    builder.setSslSocketFactory(sslSocketFactory);
    builder.setSenderMaxRetryCount(0);
    try (Fluency fluency = builder.build("192.168.0.99", 54321)) {
      fluency.emit("mytag", ImmutableMap.of("hello", "world"));
    }
    verify(socket).connect(any(InetSocketAddress.class), anyInt());
  }

  @Test
  void buildWithComplexConfig() throws IOException {
    String tmpdir = System.getProperty("java.io.tmpdir");
    assertThat(tmpdir).isNotNull();

    FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
    builder.setFlushAttemptIntervalMillis(200);
    builder.setMaxBufferSize(Long.MAX_VALUE);
    builder.setBufferChunkInitialSize(7 * 1024 * 1024);
    builder.setBufferChunkRetentionSize(13 * 1024 * 1024);
    builder.setBufferChunkRetentionTimeMillis(19 * 1000);
    builder.setJvmHeapBufferMode(true);
    builder.setSenderMaxRetryCount(99);
    builder.setSenderBaseRetryIntervalMillis(20);
    builder.setSenderMaxRetryIntervalMillis(100000);
    builder.setConnectionTimeoutMilli(12345);
    builder.setReadTimeoutMilli(9876);
    builder.setAckResponseMode(true);
    builder.setWaitUntilBufferFlushed(42);
    builder.setWaitUntilFlusherTerminated(24);
    builder.setFileBackupDir(tmpdir);

    try (Fluency fluency =
        builder.build(
            Arrays.asList(
                new InetSocketAddress("333.333.333.333", 11111),
                new InetSocketAddress("444.444.444.444", 22222)))) {

      assertThat(fluency.getBuffer()).isInstanceOf(Buffer.class);
      Buffer buffer = fluency.getBuffer();
      assertThat(buffer.getMaxBufferSize()).isEqualTo(Long.MAX_VALUE);
      assertThat(buffer.getFileBackupDir()).isEqualTo(tmpdir);
      assertThat(buffer.bufferFormatType()).isEqualTo("packed_forward");
      assertThat(buffer.getChunkRetentionTimeMillis()).isEqualTo(19 * 1000);
      assertThat(buffer.getChunkExpandRatio()).isEqualTo(2f);
      assertThat(buffer.getChunkInitialSize()).isEqualTo(7 * 1024 * 1024);
      assertThat(buffer.getChunkRetentionSize()).isEqualTo(13 * 1024 * 1024);
      assertThat(buffer.getJvmHeapBufferMode()).isEqualTo(true);

      Flusher flusher = fluency.getFlusher();
      assertThat(flusher.isTerminated()).isFalse();
      assertThat(flusher.getFlushAttemptIntervalMillis()).isEqualTo(200);
      assertThat(flusher.getWaitUntilBufferFlushed()).isEqualTo(42);
      assertThat(flusher.getWaitUntilTerminated()).isEqualTo(24);

      assertThat(flusher.getIngester().getSender()).isInstanceOf(RetryableSender.class);
      RetryableSender retryableSender = (RetryableSender) flusher.getIngester().getSender();
      assertThat(retryableSender.getRetryStrategy())
          .isInstanceOf(ExponentialBackOffRetryStrategy.class);
      ExponentialBackOffRetryStrategy retryStrategy =
          (ExponentialBackOffRetryStrategy) retryableSender.getRetryStrategy();
      assertThat(retryStrategy.getMaxRetryCount()).isEqualTo(99);
      assertThat(retryStrategy.getBaseIntervalMillis()).isEqualTo(20);
      assertThat(retryStrategy.getMaxIntervalMillis()).isEqualTo(100000);

      assertThat(retryableSender.getBaseSender()).isInstanceOf(MultiSender.class);
      MultiSender multiSender = (MultiSender) retryableSender.getBaseSender();
      assertThat(multiSender.getSenders().size()).isEqualTo(2);

      assertThat(multiSender.getSenders().get(0)).isInstanceOf(TCPSender.class);
      {
        TCPSender sender = (TCPSender) multiSender.getSenders().get(0);
        assertThat(sender.getHost()).isEqualTo("333.333.333.333");
        assertThat(sender.getPort()).isEqualTo(11111);
        assertThat(sender.getConnectionTimeoutMilli()).isEqualTo(12345);
        assertThat(sender.getReadTimeoutMilli()).isEqualTo(9876);

        FailureDetector failureDetector = sender.getFailureDetector();
        assertThat(failureDetector.getFailureIntervalMillis()).isEqualTo(3 * 1000);
        assertThat(failureDetector.getFailureDetectStrategy())
            .isInstanceOf(PhiAccrualFailureDetectStrategy.class);
        assertThat(failureDetector.getHeartbeater()).isInstanceOf(TCPHeartbeater.class);
        {
          TCPHeartbeater hb = (TCPHeartbeater) failureDetector.getHeartbeater();
          assertThat(hb.getHost()).isEqualTo("333.333.333.333");
          assertThat(hb.getPort()).isEqualTo(11111);
        }
        assertThat(failureDetector.getHeartbeater().getIntervalMillis()).isEqualTo(1000);
      }

      assertThat(multiSender.getSenders().get(1)).isInstanceOf(TCPSender.class);
      {
        TCPSender sender = (TCPSender) multiSender.getSenders().get(1);
        assertThat(sender.getHost()).isEqualTo("444.444.444.444");
        assertThat(sender.getPort()).isEqualTo(22222);
        assertThat(sender.getConnectionTimeoutMilli()).isEqualTo(12345);
        assertThat(sender.getReadTimeoutMilli()).isEqualTo(9876);

        FailureDetector failureDetector = sender.getFailureDetector();
        assertThat(failureDetector.getFailureIntervalMillis()).isEqualTo(3 * 1000);
        assertThat(failureDetector.getFailureDetectStrategy())
            .isInstanceOf(PhiAccrualFailureDetectStrategy.class);
        assertThat(failureDetector.getHeartbeater()).isInstanceOf(TCPHeartbeater.class);
        {
          TCPHeartbeater hb = (TCPHeartbeater) failureDetector.getHeartbeater();
          assertThat(hb.getHost()).isEqualTo("444.444.444.444");
          assertThat(hb.getPort()).isEqualTo(22222);
        }
        assertThat(failureDetector.getHeartbeater().getIntervalMillis()).isEqualTo(1000);
      }
    }
  }

  @Test
  void buildWithSslAndComplexConfig() throws IOException {
    String tmpdir = System.getProperty("java.io.tmpdir");
    assertThat(tmpdir).isNotNull();

    FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
    builder.setSslEnabled(true);
    builder.setFlushAttemptIntervalMillis(200);
    builder.setMaxBufferSize(Long.MAX_VALUE);
    builder.setBufferChunkInitialSize(7 * 1024 * 1024);
    builder.setBufferChunkRetentionSize(13 * 1024 * 1024);
    builder.setJvmHeapBufferMode(true);
    builder.setSenderMaxRetryCount(99);
    builder.setSenderBaseRetryIntervalMillis(20);
    builder.setSenderMaxRetryIntervalMillis(100000);
    builder.setConnectionTimeoutMilli(12345);
    builder.setReadTimeoutMilli(9876);
    builder.setAckResponseMode(true);
    builder.setWaitUntilBufferFlushed(42);
    builder.setWaitUntilFlusherTerminated(24);
    builder.setFileBackupDir(tmpdir);

    try (Fluency fluency =
        builder.build(
            Arrays.asList(
                new InetSocketAddress("333.333.333.333", 11111),
                new InetSocketAddress("444.444.444.444", 22222)))) {

      assertThat(fluency.getFlusher().getIngester().getSender())
          .isInstanceOf(RetryableSender.class);
      RetryableSender retryableSender =
          (RetryableSender) fluency.getFlusher().getIngester().getSender();
      assertThat(retryableSender.getRetryStrategy())
          .isInstanceOf(ExponentialBackOffRetryStrategy.class);
      ExponentialBackOffRetryStrategy retryStrategy =
          (ExponentialBackOffRetryStrategy) retryableSender.getRetryStrategy();
      assertThat(retryStrategy.getMaxRetryCount()).isEqualTo(99);
      assertThat(retryStrategy.getBaseIntervalMillis()).isEqualTo(20);
      assertThat(retryStrategy.getMaxIntervalMillis()).isEqualTo(100000);

      assertThat(retryableSender.getBaseSender()).isInstanceOf(MultiSender.class);
      MultiSender multiSender = (MultiSender) retryableSender.getBaseSender();
      assertThat(multiSender.getSenders().size()).isEqualTo(2);

      assertThat(multiSender.getSenders().get(0)).isInstanceOf(SSLSender.class);
      {
        SSLSender sender = (SSLSender) multiSender.getSenders().get(0);
        assertThat(sender.getHost()).isEqualTo("333.333.333.333");
        assertThat(sender.getPort()).isEqualTo(11111);
        assertThat(sender.getConnectionTimeoutMilli()).isEqualTo(12345);
        assertThat(sender.getReadTimeoutMilli()).isEqualTo(9876);

        FailureDetector failureDetector = sender.getFailureDetector();
        assertThat(failureDetector.getFailureIntervalMillis()).isEqualTo(3 * 1000);
        assertThat(failureDetector.getFailureDetectStrategy())
            .isInstanceOf(PhiAccrualFailureDetectStrategy.class);
        assertThat(failureDetector.getHeartbeater()).isInstanceOf(SSLHeartbeater.class);
        {
          SSLHeartbeater hb = (SSLHeartbeater) failureDetector.getHeartbeater();
          assertThat(hb.getHost()).isEqualTo("333.333.333.333");
          assertThat(hb.getPort()).isEqualTo(11111);
        }
        assertThat(failureDetector.getHeartbeater().getIntervalMillis()).isEqualTo(1000);
      }

      assertThat(multiSender.getSenders().get(1)).isInstanceOf(SSLSender.class);
      {
        SSLSender sender = (SSLSender) multiSender.getSenders().get(1);
        assertThat(sender.getHost()).isEqualTo("444.444.444.444");
        assertThat(sender.getPort()).isEqualTo(22222);
        assertThat(sender.getConnectionTimeoutMilli()).isEqualTo(12345);
        assertThat(sender.getReadTimeoutMilli()).isEqualTo(9876);

        FailureDetector failureDetector = sender.getFailureDetector();
        assertThat(failureDetector.getFailureIntervalMillis()).isEqualTo(3 * 1000);
        assertThat(failureDetector.getFailureDetectStrategy())
            .isInstanceOf(PhiAccrualFailureDetectStrategy.class);
        assertThat(failureDetector.getHeartbeater()).isInstanceOf(SSLHeartbeater.class);
        {
          SSLHeartbeater hb = (SSLHeartbeater) failureDetector.getHeartbeater();
          assertThat(hb.getHost()).isEqualTo("444.444.444.444");
          assertThat(hb.getPort()).isEqualTo(22222);
        }
        assertThat(failureDetector.getHeartbeater().getIntervalMillis()).isEqualTo(1000);
      }
    }
  }

  @Test
  void defaultRecordFormatter() {
    FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
    assertThat(builder.getRecordFormatter()).isInstanceOf(FluentdRecordFormatter.class);
  }

  @Test
  void customRecordFormatter() {
    FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
    builder.setRecordFormatter(new CustomFluentdRecordFormatter());
    assertThat(builder.getRecordFormatter()).isInstanceOf(CustomFluentdRecordFormatter.class);
  }

  private static class CustomFluentdRecordFormatter extends FluentdRecordFormatter {}
}
