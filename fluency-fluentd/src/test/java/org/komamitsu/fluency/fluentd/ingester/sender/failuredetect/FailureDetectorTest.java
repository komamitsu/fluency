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

package org.komamitsu.fluency.fluentd.ingester.sender.failuredetect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FailureDetectorTest {
  private static final Logger LOG = LoggerFactory.getLogger(FailureDetectorTest.class);

  @Test
  void testIsAvailable() throws IOException, InterruptedException {
    final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.socket().bind(null);
    int localPort = serverSocketChannel.socket().getLocalPort();

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    Runnable serverTask =
        () -> {
          while (!executorService.isShutdown()) {
            try (SocketChannel accept = serverSocketChannel.accept()) {
              LOG.debug("Accepted: {}", accept);
            } catch (IOException e) {
              LOG.warn("Stab TCP server got an error", e);
            }
          }
          try {
            serverSocketChannel.close();
          } catch (IOException e) {
            LOG.warn("Failed to close serverSocketChannel", e);
          }
        };

    executorService.execute(serverTask);

    TCPHeartbeater.Config heartbeaterConfig = new TCPHeartbeater.Config();
    heartbeaterConfig.setPort(localPort);

    PhiAccrualFailureDetectStrategy.Config failureDetectorConfig =
        new PhiAccrualFailureDetectStrategy.Config();
    try (FailureDetector failureDetector =
        new FailureDetector(
            new PhiAccrualFailureDetectStrategy(failureDetectorConfig),
            new TCPHeartbeater(heartbeaterConfig))) {

      assertTrue(failureDetector.isAvailable());
      TimeUnit.SECONDS.sleep(4);
      assertTrue(failureDetector.isAvailable());

      executorService.shutdownNow();
      for (int i = 0; i < 20; i++) {
        if (!failureDetector.isAvailable()) {
          break;
        }
        TimeUnit.MILLISECONDS.sleep(500);
      }
      assertFalse(failureDetector.isAvailable());
    }
  }

  @Test
  void validateConfig() {
    FailureDetector.Config config = new FailureDetector.Config();
    config.setFailureIntervalMillis(-1);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new FailureDetector(
                mock(FailureDetectStrategy.class), mock(Heartbeater.class), config));
  }
}
