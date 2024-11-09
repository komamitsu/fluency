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

package org.komamitsu.fluency.aws.s3;

import java.time.ZoneId;
import java.util.List;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.aws.s3.ingester.AwsS3Ingester;
import org.komamitsu.fluency.aws.s3.ingester.DefaultS3DestinationDecider;
import org.komamitsu.fluency.aws.s3.ingester.S3DestinationDecider;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import org.komamitsu.fluency.aws.s3.recordformat.AwsS3RecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.CsvRecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.JsonlRecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.MessagePackRecordFormatter;
import org.msgpack.core.annotations.VisibleForTesting;
import software.amazon.awssdk.services.s3.S3Client;

public class FluencyBuilderForAwsS3 extends org.komamitsu.fluency.FluencyBuilder {
  private FormatType formatType;
  private List<String> formatCsvColumnNames;
  private String awsEndpoint;
  private String awsRegion;
  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private Integer senderRetryMax;
  private Integer senderRetryIntervalMillis;
  private Integer senderMaxRetryIntervalMillis;
  private Float senderRetryFactor;
  private Integer senderWorkBufSize;
  private boolean compressionEnabled = true;
  private String s3KeyPrefix;
  private String s3KeySuffix;
  private ZoneId s3KeyTimeZoneId;
  private S3DestinationDecider customS3DestinationDecider;

  public enum FormatType {
    MESSAGE_PACK,
    JSONL,
    CSV
  }

  public FluencyBuilderForAwsS3() {
    setBufferChunkRetentionTimeMillis(30 * 1000);
    setBufferChunkInitialSize(4 * 1024 * 1024);
    setBufferChunkRetentionSize(64 * 1024 * 1024);
  }

  public FormatType getFormatType() {
    return formatType;
  }

  public void setFormatType(FormatType formatType) {
    this.formatType = formatType;
  }

  public List<String> getFormatCsvColumnNames() {
    return formatCsvColumnNames;
  }

  public void setFormatCsvColumnNames(List<String> formatCsvColumnNames) {
    this.formatCsvColumnNames = formatCsvColumnNames;
  }

  public String getAwsEndpoint() {
    return awsEndpoint;
  }

  public void setAwsEndpoint(String awsEndpoint) {
    this.awsEndpoint = awsEndpoint;
  }

  public String getAwsRegion() {
    return awsRegion;
  }

  public void setAwsRegion(String awsRegion) {
    this.awsRegion = awsRegion;
  }

  public String getAwsAccessKeyId() {
    return awsAccessKeyId;
  }

  public void setAwsAccessKeyId(String senderAwsAccessKeyId) {
    this.awsAccessKeyId = senderAwsAccessKeyId;
  }

  public String getAwsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public void setAwsSecretAccessKey(String senderAwsSecretAccessKey) {
    this.awsSecretAccessKey = senderAwsSecretAccessKey;
  }

  public Integer getSenderRetryMax() {
    return senderRetryMax;
  }

  public void setSenderRetryMax(Integer senderRetryMax) {
    this.senderRetryMax = senderRetryMax;
  }

  public Integer getSenderRetryIntervalMillis() {
    return senderRetryIntervalMillis;
  }

  public void setSenderRetryIntervalMillis(Integer senderRetryIntervalMillis) {
    this.senderRetryIntervalMillis = senderRetryIntervalMillis;
  }

  public Integer getSenderMaxRetryIntervalMillis() {
    return senderMaxRetryIntervalMillis;
  }

  public void setSenderMaxRetryIntervalMillis(Integer senderMaxRetryIntervalMillis) {
    this.senderMaxRetryIntervalMillis = senderMaxRetryIntervalMillis;
  }

  public Float getSenderRetryFactor() {
    return senderRetryFactor;
  }

  public void setSenderRetryFactor(Float senderRetryFactor) {
    this.senderRetryFactor = senderRetryFactor;
  }

  public Integer getSenderWorkBufSize() {
    return senderWorkBufSize;
  }

  public void setSenderWorkBufSize(Integer senderWorkBufSize) {
    this.senderWorkBufSize = senderWorkBufSize;
  }

  public boolean isCompressionEnabled() {
    return compressionEnabled;
  }

  public void setCompressionEnabled(boolean compressionEnabled) {
    this.compressionEnabled = compressionEnabled;
  }

  public String getS3KeyPrefix() {
    return s3KeyPrefix;
  }

  public void setS3KeyPrefix(String s3KeyPrefix) {
    this.s3KeyPrefix = s3KeyPrefix;
  }

  public String getS3KeySuffix() {
    return s3KeySuffix;
  }

  public void setS3KeySuffix(String s3KeySuffix) {
    this.s3KeySuffix = s3KeySuffix;
  }

  public ZoneId getS3KeyTimeZoneId() {
    return s3KeyTimeZoneId;
  }

  public void setS3KeyTimeZoneId(ZoneId s3KeyTimeZoneId) {
    this.s3KeyTimeZoneId = s3KeyTimeZoneId;
  }

  public S3DestinationDecider getCustomS3DestinationDecider() {
    return customS3DestinationDecider;
  }

  public void setCustomS3DestinationDecider(S3DestinationDecider customS3DestinationDecider) {
    this.customS3DestinationDecider = customS3DestinationDecider;
  }

  private String defaultKeyPrefix(AwsS3RecordFormatter recordFormatter) {
    return "." + recordFormatter.formatName();
  }

  public Fluency build(AwsS3RecordFormatter recordFormatter, AwsS3Sender.Config senderConfig) {
    return buildFromIngester(recordFormatter, createIngester(recordFormatter, senderConfig));
  }

  public Fluency build(AwsS3RecordFormatter recordFormatter) {
    AwsS3Sender.Config senderConfig = createSenderConfig();

    return build(recordFormatter, senderConfig);
  }

  public Fluency build() {
    return build(createRecordFormatter());
  }

  @VisibleForTesting
  AwsS3Sender createSender(AwsS3Sender.Config senderConfig) {
    return new AwsS3Sender(S3Client.builder(), senderConfig);
  }

  private AwsS3Ingester createIngester(
      AwsS3RecordFormatter recordFormatter, AwsS3Sender.Config senderConfig) {
    S3DestinationDecider s3DestinationDecider;
    if (getCustomS3DestinationDecider() == null) {
      DefaultS3DestinationDecider.Config config = new DefaultS3DestinationDecider.Config();

      if (getS3KeyPrefix() != null) {
        config.setKeyPrefix(getS3KeyPrefix());
      }

      if (getS3KeySuffix() != null) {
        config.setKeySuffix(getS3KeySuffix());
      } else {
        config.setKeySuffix(
            defaultKeyPrefix(recordFormatter) + (isCompressionEnabled() ? ".gz" : ""));
      }

      if (getS3KeyTimeZoneId() != null) {
        config.setZoneId(getS3KeyTimeZoneId());
      }
      s3DestinationDecider = new DefaultS3DestinationDecider(config);
    } else {
      s3DestinationDecider = getCustomS3DestinationDecider();
    }

    AwsS3Sender sender = createSender(senderConfig);
    return new AwsS3Ingester(sender, s3DestinationDecider);
  }

  private AwsS3RecordFormatter createRecordFormatter() {
    if (formatType == null) {
      throw new IllegalArgumentException("format type must be set");
    }

    AwsS3RecordFormatter recordFormatter;
    switch (getFormatType()) {
      case MESSAGE_PACK:
        recordFormatter = new MessagePackRecordFormatter();
        break;
      case JSONL:
        recordFormatter = new JsonlRecordFormatter();
        break;
      case CSV:
        CsvRecordFormatter.Config config = new CsvRecordFormatter.Config();
        config.setColumnNames(getFormatCsvColumnNames());
        recordFormatter = new CsvRecordFormatter(config);
        break;
      default:
        throw new IllegalArgumentException("Unexpected format type: " + getFormatType());
    }

    return recordFormatter;
  }

  private AwsS3Sender.Config createSenderConfig() {
    AwsS3Sender.Config config = new AwsS3Sender.Config();

    if (getAwsEndpoint() != null) {
      config.setEndpoint(getAwsEndpoint());
    }

    if (getAwsRegion() != null) {
      config.setRegion(getAwsRegion());
    }

    if (getAwsAccessKeyId() != null) {
      config.setAwsAccessKeyId(getAwsAccessKeyId());
    }

    if (getAwsSecretAccessKey() != null) {
      config.setAwsSecretAccessKey(getAwsSecretAccessKey());
    }

    if (getSenderRetryMax() != null) {
      config.setRetryMax(getSenderRetryMax());
    }

    if (getSenderRetryIntervalMillis() != null) {
      config.setRetryIntervalMs(getSenderRetryIntervalMillis());
    }

    if (getSenderMaxRetryIntervalMillis() != null) {
      config.setMaxRetryIntervalMs(getSenderMaxRetryIntervalMillis());
    }

    if (getSenderRetryFactor() != null) {
      config.setRetryFactor(getSenderRetryFactor());
    }

    if (getErrorHandler() != null) {
      config.setErrorHandler(getErrorHandler());
    }

    if (getSenderWorkBufSize() != null) {
      config.setWorkBufSize(getSenderWorkBufSize());
    }

    config.setCompressionEnabled(isCompressionEnabled());

    return config;
  }

  @Override
  public String toString() {
    return "FluencyBuilderForAwsS3{"
        + "formatType="
        + formatType
        + ", formatCsvColumnNames="
        + formatCsvColumnNames
        + ", awsEndpoint='"
        + awsEndpoint
        + '\''
        + ", awsRegion='"
        + awsRegion
        + '\''
        + ", senderRetryMax="
        + senderRetryMax
        + ", senderRetryIntervalMillis="
        + senderRetryIntervalMillis
        + ", senderMaxRetryIntervalMillis="
        + senderMaxRetryIntervalMillis
        + ", senderRetryFactor="
        + senderRetryFactor
        + ", senderWorkBufSize="
        + senderWorkBufSize
        + ", compressionEnabled="
        + compressionEnabled
        + ", s3KeyPrefix='"
        + s3KeyPrefix
        + '\''
        + ", s3KeySuffix='"
        + s3KeySuffix
        + '\''
        + ", s3KeyTimeZoneId="
        + s3KeyTimeZoneId
        + ", customS3DestinationDecider="
        + customS3DestinationDecider
        + "} "
        + super.toString();
  }
}
