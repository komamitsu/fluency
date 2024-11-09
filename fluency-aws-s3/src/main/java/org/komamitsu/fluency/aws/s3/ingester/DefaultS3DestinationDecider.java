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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.komamitsu.fluency.validation.Validatable;

public class DefaultS3DestinationDecider implements S3DestinationDecider {
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm-ss-SSSSSS");
  private final Config config;

  public DefaultS3DestinationDecider(Config config) {
    config.validateValues();
    this.config = config;
  }

  protected String getBucket(String tag) {
    return tag;
  }

  protected String getKeyBase(Instant time) {
    return ZonedDateTime.ofInstant(time, getZoneId()).format(FORMATTER);
  }

  @Override
  public S3Destination decide(String tag, Instant time) {
    String keyPrefix = getKeyPrefix() == null ? "" : getKeyPrefix() + "/";
    String keySuffix = getKeySuffix() == null ? "" : getKeySuffix();
    return new S3Destination(getBucket(tag), keyPrefix + getKeyBase(time) + keySuffix);
  }

  public String getKeyPrefix() {
    return config.getKeyPrefix();
  }

  public String getKeySuffix() {
    return config.getKeySuffix();
  }

  public ZoneId getZoneId() {
    return config.getZoneId();
  }

  public static class Config implements Validatable {
    private String keyPrefix;

    private String keySuffix;

    private ZoneId zoneId = ZoneOffset.UTC;

    public String getKeyPrefix() {
      return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
      this.keyPrefix = keyPrefix;
    }

    public String getKeySuffix() {
      return keySuffix;
    }

    public void setKeySuffix(String keySuffix) {
      this.keySuffix = keySuffix;
    }

    public ZoneId getZoneId() {
      return zoneId;
    }

    public void setZoneId(ZoneId zoneId) {
      this.zoneId = zoneId;
    }

    void validateValues() {
      validate();
    }

    @Override
    public String toString() {
      return "Config{"
          + "keyPrefix='"
          + keyPrefix
          + '\''
          + ", keySuffix='"
          + keySuffix
          + '\''
          + ", zoneId="
          + zoneId
          + '}';
    }
  }
}
