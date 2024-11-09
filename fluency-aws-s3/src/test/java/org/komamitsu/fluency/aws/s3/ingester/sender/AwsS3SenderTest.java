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

package org.komamitsu.fluency.aws.s3.ingester.sender;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.komamitsu.fluency.RetryableException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class AwsS3SenderTest {
  @Test
  void buildClientWithDefaults() {
    AwsS3Sender.Config config = new AwsS3Sender.Config();

    S3Client s3Client = mock(S3Client.class);
    S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    doReturn(s3Client).when(s3ClientBuilder).build();

    new AwsS3Sender(s3ClientBuilder, config);

    verify(s3ClientBuilder, times(1)).build();
    verify(s3ClientBuilder, times(0)).endpointOverride(any());
    verify(s3ClientBuilder, times(0)).region(any());
    verify(s3ClientBuilder, times(0)).credentialsProvider(any());
  }

  @Test
  void buildClientWithCustomizedConfig() {
    AwsS3Sender.Config config = new AwsS3Sender.Config();
    config.setEndpoint("https://another.s3endpoi.nt");
    config.setRegion("ap-northeast-1");
    config.setAwsAccessKeyId("foo");
    config.setAwsSecretAccessKey("bar");

    S3Client s3Client = mock(S3Client.class);
    S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    doReturn(s3Client).when(s3ClientBuilder).build();
    doAnswer(
            invocation -> {
              AwsCredentialsProvider provider = invocation.getArgument(0);
              AwsCredentials awsCredentials = provider.resolveCredentials();
              assertEquals("foo", awsCredentials.accessKeyId());
              assertEquals("bar", awsCredentials.secretAccessKey());
              return null;
            })
        .when(s3ClientBuilder)
        .credentialsProvider(any());

    new AwsS3Sender(s3ClientBuilder, config);

    verify(s3ClientBuilder, times(1)).build();
    verify(s3ClientBuilder, times(1))
        .endpointOverride(eq(URI.create("https://another.s3endpoi.nt")));
    verify(s3ClientBuilder, times(1)).region(eq(Region.AP_NORTHEAST_1));
    verify(s3ClientBuilder, times(1)).credentialsProvider(any());
  }

  private void testSend(AwsS3Sender.Config config, boolean gzipCompressed, int failures)
      throws IOException {
    S3Client s3Client = mock(S3Client.class);
    S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    doReturn(s3Client).when(s3ClientBuilder).build();

    AtomicInteger retryCount = new AtomicInteger();
    doAnswer(
            invocation -> {
              PutObjectRequest request = invocation.getArgument(0);

              assertEquals("hello.world", request.bucket());
              assertEquals("2345/01/31/23/59-59-99999.data", request.key());

              RequestBody body = invocation.getArgument(1);
              try (InputStream s3In = body.contentStreamProvider().newStream();
                  InputStream in = gzipCompressed ? new GZIPInputStream(s3In) : s3In) {
                byte[] content = ByteStreams.toByteArray(in);
                assertEquals("0123456789", new String(content, StandardCharsets.UTF_8));
              }

              if (retryCount.getAndIncrement() < failures) {
                throw new RuntimeException("Something happened");
              }

              return null;
            })
        .when(s3Client)
        .putObject(any(PutObjectRequest.class), any(RequestBody.class));

    AwsS3Sender sender = new AwsS3Sender(s3ClientBuilder, config);

    sender.send(
        "hello.world",
        "2345/01/31/23/59-59-99999.data",
        ByteBuffer.wrap("0123456789".getBytes(StandardCharsets.UTF_8)));

    verify(s3Client, times(failures + 1))
        .putObject(any(PutObjectRequest.class), any(RequestBody.class));

    sender.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void send(int failures) throws IOException {
    AwsS3Sender.Config config = new AwsS3Sender.Config();
    testSend(config, true, failures);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 2})
  void sendWithoutCompression(int failures) throws IOException {
    AwsS3Sender.Config config = new AwsS3Sender.Config();
    config.setCompressionEnabled(false);

    testSend(config, false, failures);
  }

  @Test
  void sendButRetryOver() {
    AwsS3Sender.Config config = new AwsS3Sender.Config();
    config.setRetryMax(2);
    assertThrows(RetryableException.class, () -> testSend(config, true, 3));
  }

  @Test
  void close() {
    S3Client s3Client = mock(S3Client.class);
    S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    doReturn(s3Client).when(s3ClientBuilder).build();

    AwsS3Sender sender = new AwsS3Sender(s3ClientBuilder);
    sender.close();

    verify(s3Client, times(1)).close();
  }
}
