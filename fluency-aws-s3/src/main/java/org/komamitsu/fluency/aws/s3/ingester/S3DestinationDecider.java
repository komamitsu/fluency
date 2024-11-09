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

import java.time.Instant;

public interface S3DestinationDecider {
  class S3Destination {
    private final String bucket;
    private final String key;

    public S3Destination(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }

    public String getBucket() {
      return bucket;
    }

    public String getKey() {
      return key;
    }
  }

  S3Destination decide(String tag, Instant time);
}
