/*
 * Copyright 2022 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.fluentd.ingester.sender.heartbeat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnixSocketHeartbeaterTest {
  private Path socketPath;
  private ServerSocketChannel serverSocketChannel;

  @BeforeEach
  void setUp() throws IOException {
    socketPath =
        Paths.get(
            System.getProperty("java.io.tmpdir"),
            String.format("fluency-unixsocket-hb-test-%s", UUID.randomUUID()));
    UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);

    serverSocketChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
    serverSocketChannel.bind(socketAddress);
  }

  @AfterEach
  void tearDown() throws IOException {
    Files.deleteIfExists(socketPath);
  }

  @Test
  void testHeartbeaterUp() throws IOException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(2);
    Executors.newSingleThreadExecutor()
        .execute(
            () -> {
              try {
                serverSocketChannel.accept();
                latch.countDown();
              } catch (IOException e) {
                e.printStackTrace();
              }
            });

    UnixSocketHeartbeater.Config config = new UnixSocketHeartbeater.Config();
    config.setPath(socketPath);
    config.setIntervalMillis(500);
    try (UnixSocketHeartbeater heartbeater = new UnixSocketHeartbeater(config)) {
      final AtomicInteger pongCounter = new AtomicInteger();
      final AtomicInteger failureCounter = new AtomicInteger();
      heartbeater.setCallback(
          new Heartbeater.Callback() {
            @Override
            public void onHeartbeat() {
              pongCounter.incrementAndGet();
              latch.countDown();
            }

            @Override
            public void onFailure(Throwable cause) {
              failureCounter.incrementAndGet();
            }
          });
      heartbeater.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertTrue(0 < pongCounter.get() && pongCounter.get() < 3);
      assertEquals(0, failureCounter.get());
    }
  }

  @Test
  void testHeartbeaterDown() throws IOException, InterruptedException {
    serverSocketChannel.close();

    UnixSocketHeartbeater.Config config = new UnixSocketHeartbeater.Config();
    config.setPath(socketPath);
    config.setIntervalMillis(500);
    try (UnixSocketHeartbeater heartbeater = new UnixSocketHeartbeater(config)) {
      final AtomicInteger pongCounter = new AtomicInteger();
      final AtomicInteger failureCounter = new AtomicInteger();
      heartbeater.setCallback(
          new Heartbeater.Callback() {
            @Override
            public void onHeartbeat() {
              pongCounter.incrementAndGet();
            }

            @Override
            public void onFailure(Throwable cause) {
              failureCounter.incrementAndGet();
            }
          });
      heartbeater.start();
      TimeUnit.SECONDS.sleep(1);
      assertEquals(0, pongCounter.get());
      assertTrue(failureCounter.get() > 0);
    }
  }
}
