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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsS3Ingester implements Ingester {
  private static final Logger LOG = LoggerFactory.getLogger(AwsS3Ingester.class);
  private final AwsS3Sender sender;
  private final S3DestinationDecider s3DestinationDecider;

  public AwsS3Ingester(AwsS3Sender sender, S3DestinationDecider s3DestinationDecider) {
    this.sender = sender;
    this.s3DestinationDecider = s3DestinationDecider;
  }

  @Override
  public void ingest(String tag, ByteBuffer dataBuffer) throws IOException {
    S3DestinationDecider.S3Destination s3Destination =
        s3DestinationDecider.decide(tag, Instant.now());
    String bucket = s3Destination.getBucket();
    String key = s3Destination.getKey();

    sender.send(bucket, key, dataBuffer);
  }

  @Override
  public Sender getSender() {
    return sender;
  }

  public S3DestinationDecider getS3DestinationDecider() {
    return s3DestinationDecider;
  }

  @Override
  public void close() {
    sender.close();
  }
}
