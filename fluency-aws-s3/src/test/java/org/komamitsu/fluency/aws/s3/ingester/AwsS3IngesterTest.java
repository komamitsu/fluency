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

package org.komamitsu.fluency.aws.s3.ingester;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;

class AwsS3IngesterTest {
  private AwsS3Sender s3Sender;
  private S3DestinationDecider destinationDecider;
  private AwsS3Ingester ingester;

  @BeforeEach
  void setUp() {
    s3Sender = mock(AwsS3Sender.class);

    destinationDecider = mock(S3DestinationDecider.class);
    doReturn(new S3DestinationDecider.S3Destination("mybucket", "my/key/base.data"))
        .when(destinationDecider)
        .decide(anyString(), any(Instant.class));

    ingester = new AwsS3Ingester(s3Sender, destinationDecider);
  }

  @Test
  void ingest() throws IOException {
    ingester.ingest("foo.bar", ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8)));

    verify(s3Sender, times(1))
        .send(
            eq("mybucket"),
            eq("my/key/base.data"),
            eq(ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  void close() {
    ingester.close();

    verify(s3Sender, times(1)).close();
  }
}
