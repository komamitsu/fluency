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

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class DefaultS3DestinationDeciderTest {
  // +09:00
  private static ZoneId TIMEZONE_JST = ZoneId.of("JST", ZoneId.SHORT_IDS);

  @Test
  void decide() {
    DefaultS3DestinationDecider.Config config = new DefaultS3DestinationDecider.Config();
    config.setKeyPrefix("archives");
    config.setKeySuffix(".testdata");
    DefaultS3DestinationDecider decider = new DefaultS3DestinationDecider(config);
    ZonedDateTime time = ZonedDateTime.of(2019, 12, 31, 23, 59, 59, 999999000, TIMEZONE_JST);
    S3DestinationDecider.S3Destination destination =
        decider.decide("web.access_log", time.toInstant());
    assertEquals("web.access_log", destination.getBucket());
    // JST is 9 hours ahead of UTC
    assertEquals("archives/2019/12/31/14/59-59-999999.testdata", destination.getKey());
  }

  @Test
  void decideWithSpecificTimeZone() {
    DefaultS3DestinationDecider.Config config = new DefaultS3DestinationDecider.Config();
    config.setKeyPrefix("archives");
    config.setKeySuffix(".testdata");
    config.setZoneId(TIMEZONE_JST);
    DefaultS3DestinationDecider decider = new DefaultS3DestinationDecider(config);
    ZonedDateTime time = ZonedDateTime.of(2019, 12, 31, 23, 59, 59, 999999000, TIMEZONE_JST);
    S3DestinationDecider.S3Destination destination =
        decider.decide("web.access_log", time.toInstant());
    assertEquals("web.access_log", destination.getBucket());
    assertEquals("archives/2019/12/31/23/59-59-999999.testdata", destination.getKey());
  }

  @Test
  void decideWithoutPrefixNorSuffix() {
    DefaultS3DestinationDecider.Config config = new DefaultS3DestinationDecider.Config();
    DefaultS3DestinationDecider decider = new DefaultS3DestinationDecider(config);
    ZonedDateTime time = ZonedDateTime.of(2019, 12, 31, 23, 59, 59, 999999000, TIMEZONE_JST);
    S3DestinationDecider.S3Destination destination =
        decider.decide("web.access_log", time.toInstant());
    assertEquals("web.access_log", destination.getBucket());
    // JST is 9 hours ahead of UTC
    assertEquals("2019/12/31/14/59-59-999999", destination.getKey());
  }
}
