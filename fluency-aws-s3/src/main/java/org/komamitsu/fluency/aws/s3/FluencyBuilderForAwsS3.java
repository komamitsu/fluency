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

import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.aws.s3.ingester.DefaultS3DestinationDecider;
import org.komamitsu.fluency.aws.s3.ingester.S3DestinationDecider;
import org.komamitsu.fluency.aws.s3.recordformat.AwsS3RecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.CsvRecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.JsonlRecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.MessagePackRecordFormatter;
import org.komamitsu.fluency.aws.s3.ingester.AwsS3Ingester;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.ZoneId;
import java.util.List;

public class FluencyBuilderForAwsS3
        extends org.komamitsu.fluency.FluencyBuilder
{
    private FormatType formatType;
    private List<String> formatCsvColumnNames;

    private String senderEndpoint;
    private String senderRegion;
    private String senderAwsAccessKeyId;
    private String senderAwsSecretAccessKey;
    private Integer senderRetryMax;
    private Integer senderRetryIntervalMillis;
    private Integer senderMaxRetryIntervalMillis;
    private Float senderRetryFactor;
    private Integer senderWorkBufSize;
    private String ingesterKeySuffix;
    private ZoneId s3DestinationDeciderTimeZoneId;
    private S3DestinationDecider customS3DestinationDecider;

    public enum FormatType {
        MESSAGE_PACK,
        JSONL,
        CSV
    }

    public FluencyBuilderForAwsS3()
    {
        setBufferChunkRetentionTimeMillis(30 * 1000);
        setBufferChunkInitialSize(4 * 1024 * 1024);
        setBufferChunkRetentionSize(64 * 1024 * 1024);
    }

    public FormatType getFormatType()
    {
        return formatType;
    }

    public void setFormatType(FormatType formatType)
    {
        this.formatType = formatType;
    }

    public List<String> getFormatCsvColumnNames()
    {
        return formatCsvColumnNames;
    }

    public void setFormatCsvColumnNames(List<String> formatCsvColumnNames)
    {
        this.formatCsvColumnNames = formatCsvColumnNames;
    }

    public String getSenderEndpoint()
    {
        return senderEndpoint;
    }

    public void setSenderEndpoint(String senderEndpoint)
    {
        this.senderEndpoint = senderEndpoint;
    }

    public String getSenderRegion()
    {
        return senderRegion;
    }

    public void setSenderRegion(String senderRegion)
    {
        this.senderRegion = senderRegion;
    }

    public String getSenderAwsAccessKeyId()
    {
        return senderAwsAccessKeyId;
    }

    public void setSenderAwsAccessKeyId(String senderAwsAccessKeyId)
    {
        this.senderAwsAccessKeyId = senderAwsAccessKeyId;
    }

    public String getSenderAwsSecretAccessKey()
    {
        return senderAwsSecretAccessKey;
    }

    public void setSenderAwsSecretAccessKey(String senderAwsSecretAccessKey)
    {
        this.senderAwsSecretAccessKey = senderAwsSecretAccessKey;
    }

    public Integer getSenderRetryMax()
    {
        return senderRetryMax;
    }

    public void setSenderRetryMax(Integer senderRetryMax)
    {
        this.senderRetryMax = senderRetryMax;
    }

    public Integer getSenderRetryIntervalMillis()
    {
        return senderRetryIntervalMillis;
    }

    public void setSenderRetryIntervalMillis(Integer senderRetryIntervalMillis)
    {
        this.senderRetryIntervalMillis = senderRetryIntervalMillis;
    }

    public Integer getSenderMaxRetryIntervalMillis()
    {
        return senderMaxRetryIntervalMillis;
    }

    public void setSenderMaxRetryIntervalMillis(Integer senderMaxRetryIntervalMillis)
    {
        this.senderMaxRetryIntervalMillis = senderMaxRetryIntervalMillis;
    }

    public Float getSenderRetryFactor()
    {
        return senderRetryFactor;
    }

    public void setSenderRetryFactor(Float senderRetryFactor)
    {
        this.senderRetryFactor = senderRetryFactor;
    }

    public Integer getSenderWorkBufSize()
    {
        return senderWorkBufSize;
    }

    public void setSenderWorkBufSize(Integer senderWorkBufSize)
    {
        this.senderWorkBufSize = senderWorkBufSize;
    }

    public String getIngesterKeySuffix()
    {
        return ingesterKeySuffix;
    }

    public void setIngesterKeySuffix(String ingesterKeySuffix)
    {
        this.ingesterKeySuffix = ingesterKeySuffix;
    }

    public ZoneId getS3DestinationDeciderTimeZoneId()
    {
        return s3DestinationDeciderTimeZoneId;
    }

    public void setS3DestinationDeciderTimeZoneId(ZoneId s3DestinationDeciderTimeZoneId)
    {
        this.s3DestinationDeciderTimeZoneId = s3DestinationDeciderTimeZoneId;
    }

    public S3DestinationDecider getCustomS3DestinationDecider()
    {
        return customS3DestinationDecider;
    }

    public void setCustomS3DestinationDecider(S3DestinationDecider customS3DestinationDecider)
    {
        this.customS3DestinationDecider = customS3DestinationDecider;
    }

    private String defaultKeyPrefix(AwsS3RecordFormatter recordFormatter)
    {
        return "." + recordFormatter.formatName();
    }

    public Fluency build(AwsS3RecordFormatter recordFormatter, AwsS3Sender.Config senderConfig)
    {
        AwsS3Ingester.Config ingesterConfig = new AwsS3Ingester.Config();

        if (getIngesterKeySuffix() == null) {
            ingesterConfig.setKeySuffix(defaultKeyPrefix(recordFormatter) + ".gz");
        }
        else {
            ingesterConfig.setKeySuffix(getIngesterKeySuffix());
        }

        S3DestinationDecider s3DestinationDecider;
        if (getCustomS3DestinationDecider() == null) {
            DefaultS3DestinationDecider.Config config = new DefaultS3DestinationDecider.Config();
            if (getS3DestinationDeciderTimeZoneId() != null) {
                config.setZoneId(getS3DestinationDeciderTimeZoneId());
            }
            s3DestinationDecider = new DefaultS3DestinationDecider(config);
        }
        else {
            s3DestinationDecider = getCustomS3DestinationDecider();
        }

        AwsS3Sender sender = new AwsS3Sender(S3Client.builder(), senderConfig);
        AwsS3Ingester ingester = new AwsS3Ingester(ingesterConfig, sender, s3DestinationDecider);

        return buildFromIngester(recordFormatter, ingester);
    }

    public Fluency build()
    {
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

        return build(recordFormatter);
    }

    public Fluency build(AwsS3RecordFormatter recordFormatter)
    {
        AwsS3Sender.Config senderConfig = createSenderConfig();


        return build(recordFormatter, senderConfig);
    }

    private AwsS3Sender.Config createSenderConfig()
    {
        AwsS3Sender.Config senderConfig = new AwsS3Sender.Config();
        if (getSenderEndpoint() != null) {
            senderConfig.setEndpoint(getSenderEndpoint());
        }
        if (getSenderRegion()!= null) {
            senderConfig.setRegion(getSenderRegion());
        }
        if (getSenderAwsAccessKeyId() != null) {
            senderConfig.setAwsAccessKeyId(getSenderAwsAccessKeyId());
        }
        if (getSenderAwsSecretAccessKey() != null) {
            senderConfig.setAwsSecretAccessKey(getSenderAwsSecretAccessKey());
        }
        if (getSenderRetryMax() != null) {
            senderConfig.setRetryMax(getSenderRetryMax());
        }
        if (getSenderRetryIntervalMillis() != null) {
            senderConfig.setRetryIntervalMs(getSenderRetryIntervalMillis());
        }
        if (getSenderMaxRetryIntervalMillis() != null) {
            senderConfig.setMaxRetryIntervalMs(getSenderMaxRetryIntervalMillis());
        }
        if (getSenderRetryFactor() != null) {
            senderConfig.setRetryFactor(getSenderRetryFactor());
        }
        if (getErrorHandler() != null) {
            senderConfig.setErrorHandler(getErrorHandler());
        }
        if (getSenderWorkBufSize() != null) {
            senderConfig.setWorkBufSize(getSenderWorkBufSize());
        }

        return senderConfig;
    }

    @Override
    public String toString()
    {
        return "FluencyBuilder{" +
                "senderRetryMax=" + senderRetryMax +
                ", senderRetryIntervalMillis=" + senderRetryIntervalMillis +
                ", senderMaxRetryIntervalMillis=" + senderMaxRetryIntervalMillis +
                ", senderRetryFactor=" + senderRetryFactor +
                ", senderWorkBufSize=" + senderWorkBufSize +
                "} " + super.toString();
    }
}
