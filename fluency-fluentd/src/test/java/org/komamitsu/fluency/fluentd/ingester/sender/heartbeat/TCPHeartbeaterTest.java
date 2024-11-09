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

package org.komamitsu.fluency.fluentd.ingester.sender.heartbeat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TCPHeartbeaterTest {
  @Test
  void testTCPHeartbeaterUp() throws IOException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(2);
    final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.socket().bind(null);
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

    TCPHeartbeater.Config config = new TCPHeartbeater.Config();
    config.setPort(serverSocketChannel.socket().getLocalPort());
    config.setIntervalMillis(500);
    try (TCPHeartbeater heartbeater = new TCPHeartbeater(config)) {
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
  void testTCPHeartbeaterDown() throws IOException, InterruptedException {
    final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.socket().bind(null);
    int localPort = serverSocketChannel.socket().getLocalPort();
    serverSocketChannel.close();

    TCPHeartbeater.Config config = new TCPHeartbeater.Config();
    config.setPort(localPort);
    config.setIntervalMillis(500);
    try (TCPHeartbeater heartbeater = new TCPHeartbeater(config)) {
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
