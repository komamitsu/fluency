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
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.komamitsu.fluency.aws.s3.ingester.AwsS3Ingester;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import org.komamitsu.fluency.aws.s3.recordformat.AwsS3RecordFormatter;

public class FluencyBuilderForAwsS3
        extends org.komamitsu.fluency.FluencyBuilder
{
    private String senderEndpoint;
    private String senderRegion;
    private String senderAwsAccessKeyId;
    private String senderAwsSecretAccessKey;
    private Integer senderRetryMax;
    private Integer senderRetryIntervalMillis;
    private Integer senderMaxRetryIntervalMillis;
    private Float senderRetryFactor;
    private Integer senderWorkBufSize;

    public FluencyBuilderForAwsS3()
    {
        setBufferChunkRetentionTimeMillis(30 * 1000);
        setBufferChunkInitialSize(4 * 1024 * 1024);
        setBufferChunkRetentionSize(64 * 1024 * 1024);
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

    public Fluency build()
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createSenderConfig()));
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

    private RecordFormatter buildRecordFormatter()
    {
        return new AwsS3RecordFormatter();
    }

    private Ingester buildIngester(AwsS3Sender.Config senderConfig)
    {
        AwsS3Sender sender = new AwsS3Sender(senderConfig);
        return new AwsS3Ingester(sender);
    }
}
