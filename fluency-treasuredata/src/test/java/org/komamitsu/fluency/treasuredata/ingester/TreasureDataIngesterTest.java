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

package org.komamitsu.fluency.treasuredata.ingester;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;
import org.mockito.ArgumentCaptor;

class TreasureDataIngesterTest {
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final String TAG = "foo.bar";
  private static final byte[] DATA = "hello, world".getBytes(CHARSET);
  private TreasureDataSender treasureDataSender;

  @BeforeEach
  void setUp() throws Exception {
    treasureDataSender = mock(TreasureDataSender.class);
  }

  @Test
  void ingest() throws IOException {
    TreasureDataIngester ingester = new TreasureDataIngester(treasureDataSender);
    ingester.ingest(TAG, ByteBuffer.wrap(DATA));
    ArgumentCaptor<ByteBuffer> byteBufferArgumentCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(treasureDataSender, times(1)).send(eq(TAG), byteBufferArgumentCaptor.capture());

    assertEquals(1, byteBufferArgumentCaptor.getAllValues().size());
    assertArrayEquals(DATA, byteBufferArgumentCaptor.getAllValues().get(0).array());
  }

  @Test
  void getSender() {
    assertEquals(treasureDataSender, new TreasureDataIngester(treasureDataSender).getSender());
  }

  @Test
  void close() throws IOException {
    TreasureDataIngester ingester = new TreasureDataIngester(treasureDataSender);
    ingester.close();

    verify(treasureDataSender, times(1)).close();
  }
}
