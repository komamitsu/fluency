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

package org.komamitsu.fluency.fluentd.ingester.sender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.fluentd.MockTCPServer;
import org.komamitsu.fluency.fluentd.MockTCPServerWithMetrics;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TCPSenderTest {
  private static final Logger LOG = LoggerFactory.getLogger(TCPSenderTest.class);

  @Test
  void testSend() throws Exception {
    testSendBase(
        port -> {
          TCPSender.Config senderConfig = new TCPSender.Config();
          senderConfig.setPort(port);
          return new TCPSender(senderConfig);
        },
        count -> assertThat(count).isEqualTo(1),
        count -> assertThat(count).isEqualTo(1));
  }

  @Test
  void testSendWithHeartbeart() throws Exception {
    testSendBase(
        port -> {
          TCPHeartbeater.Config hbConfig = new TCPHeartbeater.Config();
          hbConfig.setPort(port);
          hbConfig.setIntervalMillis(400);

          TCPSender.Config senderConfig = new TCPSender.Config();
          senderConfig.setPort(port);

          return new TCPSender(
              senderConfig,
              new FailureDetector(
                  new PhiAccrualFailureDetectStrategy(), new TCPHeartbeater(hbConfig)));
        },
        count -> assertThat(count).isGreaterThan(1),
        count -> assertThat(count).isGreaterThan(1));
  }

  private void testSendBase(
      TCPSenderCreater senderCreater,
      Consumer<Integer> connectCountAssertion,
      Consumer<Integer> closeCountAssertion)
      throws Exception {
    MockTCPServerWithMetrics server = new MockTCPServerWithMetrics(false);
    server.start();

    int concurency = 20;
    final int reqNum = 5000;
    final CountDownLatch latch = new CountDownLatch(concurency);
    TCPSender sender = senderCreater.create(server.getLocalPort());

    // To receive heartbeat at least once
    TimeUnit.MILLISECONDS.sleep(500);

    final ExecutorService senderExecutorService = Executors.newCachedThreadPool();
    for (int i = 0; i < concurency; i++) {
      senderExecutorService.execute(
          () -> {
            try {
              byte[] bytes = "0123456789".getBytes(Charset.forName("UTF-8"));

              for (int j = 0; j < reqNum; j++) {
                sender.send(ByteBuffer.wrap(bytes));
              }
              latch.countDown();
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    }

    assertTrue(latch.await(4, TimeUnit.SECONDS));
    sender.close();

    server.waitUntilEventsStop();
    server.stop();

    int connectCount = 0;
    int closeCount = 0;
    long recvCount = 0;
    long recvLen = 0;
    for (Tuple<MockTCPServerWithMetrics.Type, Integer> event : server.getEvents()) {
      switch (event.getFirst()) {
        case CONNECT:
          connectCount++;
          break;
        case CLOSE:
          closeCount++;
          break;
        case RECEIVE:
          recvCount++;
          recvLen += event.getSecond();
          break;
      }
    }
    LOG.debug("recvCount={}", recvCount);

    connectCountAssertion.accept(connectCount);
    assertThat(recvLen).isEqualTo((long) concurency * reqNum * 10);
    closeCountAssertion.accept(closeCount);
  }

  @Test
  void testConnectionTimeout() throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(
        () -> {
          TCPSender.Config senderConfig = new TCPSender.Config();
          senderConfig.setHost("192.0.2.0");
          senderConfig.setConnectionTimeoutMilli(1000);
          TCPSender sender = new TCPSender(senderConfig);
          try {
            sender.send(ByteBuffer.wrap("hello, world".getBytes("UTF-8")));
          } catch (Throwable e) {
            if (e instanceof SocketTimeoutException) {
              latch.countDown();
            } else {
              throw new RuntimeException(e);
            }
          }
        });
    assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
  }

  @Test
  void testReadTimeout() throws Exception {
    final MockTCPServer server = new MockTCPServer(false);
    server.start();

    try {
      final CountDownLatch latch = new CountDownLatch(1);
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.execute(
          () -> {
            TCPSender.Config senderConfig = new TCPSender.Config();
            senderConfig.setPort(server.getLocalPort());
            senderConfig.setReadTimeoutMilli(1000);
            TCPSender sender = new TCPSender(senderConfig);
            try {
              sender.sendWithAck(
                  Arrays.asList(ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8))),
                  "Waiting ack forever");
            } catch (Throwable e) {
              if (e instanceof SocketTimeoutException) {
                latch.countDown();
              } else {
                throw new RuntimeException(e);
              }
            }
          });
      assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
    } finally {
      server.stop();
    }
  }

  private Throwable extractRootCause(Throwable exception) {
    Throwable e = exception;
    while (e.getCause() != null) {
      e = e.getCause();
    }
    return e;
  }

  @Test
  void testDisconnBeforeRecv() throws Exception {
    final MockTCPServer server = new MockTCPServer(false);
    server.start();

    try {
      final CountDownLatch latch = new CountDownLatch(1);
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.execute(
          () -> {
            TCPSender.Config senderConfig = new TCPSender.Config();
            senderConfig.setPort(server.getLocalPort());
            senderConfig.setReadTimeoutMilli(4000);
            TCPSender sender = new TCPSender(senderConfig);
            try {
              sender.sendWithAck(
                  Arrays.asList(ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8))),
                  "Waiting ack forever");
            } catch (Throwable e) {
              Throwable rootCause = extractRootCause(e);
              if (rootCause instanceof SocketException
                  && rootCause.getMessage().toLowerCase().contains("disconnected")) {
                latch.countDown();
              } else {
                throw new RuntimeException(e);
              }
            }
          });

      TimeUnit.MILLISECONDS.sleep(1000);
      server.stop(true);

      assertTrue(latch.await(8000, TimeUnit.MILLISECONDS));
    } finally {
      server.stop();
    }
  }

  @Test
  void testClose() throws Exception {
    final MockTCPServer server = new MockTCPServer(false);
    server.start();

    try {
      final AtomicLong duration = new AtomicLong();
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<Void> future =
          executorService.submit(
              () -> {
                TCPSender.Config senderConfig = new TCPSender.Config();
                senderConfig.setPort(server.getLocalPort());
                senderConfig.setWaitBeforeCloseMilli(1500);
                TCPSender sender = new TCPSender(senderConfig);
                long start;
                try {
                  sender.send(Arrays.asList(ByteBuffer.wrap("hello, world".getBytes("UTF-8"))));
                  start = System.currentTimeMillis();
                  sender.close();
                  duration.set(System.currentTimeMillis() - start);
                } catch (Exception e) {
                  LOG.error("Unexpected exception", e);
                }

                return null;
              });
      future.get(3000, TimeUnit.MILLISECONDS);
      assertTrue(duration.get() > 1000 && duration.get() < 2000);
    } finally {
      server.stop();
    }
  }

  @Test
  void testConfig() {
    TCPSender.Config config = new TCPSender.Config();
    assertEquals(1000, config.getWaitBeforeCloseMilli());
    // TODO: Add others later
  }

  @Test
  void validateConfig() {
    {
      TCPSender.Config config = new TCPSender.Config();
      config.setConnectionTimeoutMilli(9);
      assertThrows(IllegalArgumentException.class, () -> new TCPSender(config));
    }

    {
      TCPSender.Config config = new TCPSender.Config();
      config.setReadTimeoutMilli(9);
      assertThrows(IllegalArgumentException.class, () -> new TCPSender(config));
    }

    {
      TCPSender.Config config = new TCPSender.Config();
      config.setWaitBeforeCloseMilli(-1);
      assertThrows(IllegalArgumentException.class, () -> new TCPSender(config));
    }
  }

  interface TCPSenderCreater {
    TCPSender create(int port);
  }
}
