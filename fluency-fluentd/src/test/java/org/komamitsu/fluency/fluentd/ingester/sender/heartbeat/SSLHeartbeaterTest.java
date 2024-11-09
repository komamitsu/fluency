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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.fluentd.SSLTestSocketFactories;

public class SSLHeartbeaterTest {
  private SSLServerSocket sslServerSocket;
  private SSLHeartbeater heartbeater;

  @BeforeEach
  void setUp()
      throws CertificateException,
          UnrecoverableKeyException,
          NoSuchAlgorithmException,
          IOException,
          KeyManagementException,
          KeyStoreException {
    sslServerSocket = SSLTestSocketFactories.createServerSocket();

    SSLHeartbeater.Config config = new SSLHeartbeater.Config();
    config.setPort(sslServerSocket.getLocalPort());
    config.setIntervalMillis(500);
    heartbeater = new SSLHeartbeater(config);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (sslServerSocket != null && !sslServerSocket.isClosed()) {
      sslServerSocket.close();
    }
  }

  @Test
  void testHeartbeaterUp() throws IOException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(2);
    Executors.newSingleThreadExecutor()
        .execute(
            () -> {
              try {
                sslServerSocket.accept();
                latch.countDown();
              } catch (IOException e) {
                e.printStackTrace();
              }
            });

    final AtomicInteger pongCounter = new AtomicInteger();
    final AtomicInteger failureCounter = new AtomicInteger();
    try {
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
      assertTrue(latch.await(10, TimeUnit.SECONDS));
      assertTrue(0 < pongCounter.get() && pongCounter.get() < 3);
      assertEquals(0, failureCounter.get());
    } finally {
      if (heartbeater != null) {
        heartbeater.close();
      }
    }
  }

  @Test
  public void testHeartbeaterDown() throws IOException, InterruptedException {
    sslServerSocket.close();

    final AtomicInteger pongCounter = new AtomicInteger();
    final AtomicInteger failureCounter = new AtomicInteger();
    try {
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
    } finally {
      if (heartbeater != null) {
        heartbeater.close();
      }
    }
  }

  @Test
  void validateConfig() {
    {
      SSLHeartbeater.Config config = new SSLHeartbeater.Config();
      config.setIntervalMillis(99);
      assertThrows(IllegalArgumentException.class, () -> new SSLHeartbeater(config));
    }

    {
      SSLHeartbeater.Config config = new SSLHeartbeater.Config();
      config.setConnectionTimeoutMilli(9);
      assertThrows(IllegalArgumentException.class, () -> new SSLHeartbeater(config));
    }

    {
      SSLHeartbeater.Config config = new SSLHeartbeater.Config();
      config.setReadTimeoutMilli(9);
      assertThrows(IllegalArgumentException.class, () -> new SSLHeartbeater(config));
    }
  }
}
