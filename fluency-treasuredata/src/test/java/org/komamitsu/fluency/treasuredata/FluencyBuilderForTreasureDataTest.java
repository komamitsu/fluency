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

package org.komamitsu.fluency.treasuredata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.TDHttpClient;
import java.io.IOException;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;

class FluencyBuilderForTreasureDataTest {
  private static final String APIKEY = "12345/1qaz2wsx3edc4rfv5tgb6yhn";

  private void assertBuffer(Buffer buffer) {
    assertEquals(512 * 1024 * 1024L, buffer.getMaxBufferSize());
    assertNull(buffer.getFileBackupDir());
    assertEquals("packed_forward", buffer.bufferFormatType());
    assertEquals(2f, buffer.getChunkExpandRatio());
    assertEquals(64 * 1024 * 1024, buffer.getChunkRetentionSize());
    assertEquals(4 * 1024 * 1024, buffer.getChunkInitialSize());
    assertEquals(30000, buffer.getChunkRetentionTimeMillis());
    assertFalse(buffer.getJvmHeapBufferMode());
  }

  private void assertFlusher(Flusher flusher) {
    assertFalse(flusher.isTerminated());
    assertEquals(600, flusher.getFlushAttemptIntervalMillis());
    assertEquals(60, flusher.getWaitUntilBufferFlushed());
    assertEquals(60, flusher.getWaitUntilTerminated());
  }

  private void assertDefaultFluentdSender(
      TreasureDataSender sender, String expectedEndpoint, boolean expectedUseSsl)
      throws NoSuchFieldException, IllegalAccessException {
    assertEquals(1000, sender.getRetryInternalMs());
    assertEquals(30000, sender.getMaxRetryInternalMs());
    assertEquals(2.0f, sender.getRetryFactor());
    assertEquals(10, sender.getRetryMax());
    assertEquals(8192, sender.getWorkBufSize());

    Field httpClientField = TDClient.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    TDHttpClient tdHttpClient = (TDHttpClient) httpClientField.get(sender.getClient());

    Field configField = TDHttpClient.class.getDeclaredField("config");
    configField.setAccessible(true);
    TDClientConfig config = (TDClientConfig) configField.get(tdHttpClient);

    assertEquals(expectedEndpoint, config.endpoint);
    assertEquals(expectedUseSsl, config.useSSL);
    assertEquals(FluencyBuilderForTreasureDataTest.APIKEY, config.apiKey.get());
  }

  @Test
  void build() throws IOException, NoSuchFieldException, IllegalAccessException {
    try (Fluency fluency = new FluencyBuilderForTreasureData().build(APIKEY)) {
      assertBuffer(fluency.getBuffer());
      assertFlusher(fluency.getFlusher());
      assertDefaultFluentdSender(
          (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
          "api-import.treasuredata.com",
          true);
    }
  }

  @Test
  void buildWithCustomHttpsEndpoint()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    try (Fluency fluency =
        new FluencyBuilderForTreasureData().build(APIKEY, "https://custom.endpoint.org")) {
      assertBuffer(fluency.getBuffer());
      assertFlusher(fluency.getFlusher());
      assertDefaultFluentdSender(
          (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
          "custom.endpoint.org",
          true);
    }
  }

  @Test
  void buildWithCustomHttpsEndpointWithoutScheme()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    try (Fluency fluency =
        new FluencyBuilderForTreasureData().build(APIKEY, "custom.endpoint.org")) {
      assertBuffer(fluency.getBuffer());
      assertFlusher(fluency.getFlusher());
      assertDefaultFluentdSender(
          (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
          "custom.endpoint.org",
          true);
    }
  }

  @Test
  void buildWithCustomHttpEndpoint()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    try (Fluency fluency =
        new FluencyBuilderForTreasureData().build(APIKEY, "http://custom.endpoint.org")) {
      assertBuffer(fluency.getBuffer());
      assertFlusher(fluency.getFlusher());
      assertDefaultFluentdSender(
          (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
          "custom.endpoint.org",
          false);
    }
  }

  @Test
  void buildWithAllCustomConfig() throws IOException {
    String tmpdir = System.getProperty("java.io.tmpdir");
    assertNotNull(tmpdir);

    FluencyBuilderForTreasureData builder = new FluencyBuilderForTreasureData();
    builder.setFlushAttemptIntervalMillis(200);
    builder.setMaxBufferSize(Long.MAX_VALUE);
    builder.setBufferChunkInitialSize(7 * 1024 * 1024);
    builder.setBufferChunkRetentionSize(13 * 1024 * 1024);
    builder.setBufferChunkRetentionTimeMillis(19 * 1000);
    builder.setJvmHeapBufferMode(true);
    builder.setWaitUntilBufferFlushed(42);
    builder.setWaitUntilFlusherTerminated(24);
    builder.setFileBackupDir(tmpdir);
    builder.setSenderRetryIntervalMillis(1234);
    builder.setSenderMaxRetryIntervalMillis(345678);
    builder.setSenderRetryFactor(3.14f);
    builder.setSenderRetryMax(17);
    builder.setSenderWorkBufSize(123456);
    ;

    try (Fluency fluency = builder.build(APIKEY)) {
      assertEquals(Buffer.class, fluency.getBuffer().getClass());
      Buffer buffer = fluency.getBuffer();
      assertEquals(Long.MAX_VALUE, buffer.getMaxBufferSize());
      assertEquals(tmpdir, buffer.getFileBackupDir());
      assertEquals("packed_forward", buffer.bufferFormatType());
      assertEquals(19 * 1000, buffer.getChunkRetentionTimeMillis());
      assertEquals(2f, buffer.getChunkExpandRatio());
      assertEquals(7 * 1024 * 1024, buffer.getChunkInitialSize());
      assertEquals(13 * 1024 * 1024, buffer.getChunkRetentionSize());
      assertTrue(buffer.getJvmHeapBufferMode());

      Flusher flusher = fluency.getFlusher();
      assertFalse(flusher.isTerminated());
      assertEquals(200, flusher.getFlushAttemptIntervalMillis());
      assertEquals(42, flusher.getWaitUntilBufferFlushed());
      assertEquals(24, flusher.getWaitUntilTerminated());

      assertEquals(TreasureDataSender.class, flusher.getIngester().getSender().getClass());
      TreasureDataSender sender = (TreasureDataSender) flusher.getIngester().getSender();
      assertEquals(1234, sender.getRetryInternalMs());
      assertEquals(345678, sender.getMaxRetryInternalMs());
      assertEquals(3.14f, sender.getRetryFactor());
      assertEquals(17, sender.getRetryMax());
      assertEquals(123456, sender.getWorkBufSize());
    }
  }
}
