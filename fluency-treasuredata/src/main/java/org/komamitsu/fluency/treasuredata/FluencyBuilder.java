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

package org.komamitsu.fluency.treasuredata;

import org.komamitsu.fluency.BaseFluencyBuilder;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.komamitsu.fluency.treasuredata.ingester.TreasureDataIngester;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;
import org.komamitsu.fluency.treasuredata.recordformat.TreasureDataRecordFormatter;

public class FluencyBuilder
        extends BaseFluencyBuilder
{
    private Integer senderRetryMax;
    private Integer senderRetryIntervalMillis;
    private Integer senderMaxRetryIntervalMillis;
    private Float senderRetryFactor;
    private Integer senderWorkBufSize;

    public FluencyBuilder()
    {
        setBufferChunkRetentionTimeMillis(30 * 1000);
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

    public Fluency build(String apikey, String endpoint)
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createSenderConfig(endpoint, apikey)));
    }

    public Fluency build(String apikey)
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createSenderConfig(null, apikey)));
    }

    private TreasureDataSender.Config createSenderConfig(String endpoint, String apikey)
    {
        if (apikey == null) {
            throw new IllegalArgumentException("`apikey` should be set");
        }

        TreasureDataSender.Config senderConfig = new TreasureDataSender.Config();
        senderConfig.setApikey(apikey);

        if (endpoint != null) {
            senderConfig.setEndpoint(endpoint);
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
        return new TreasureDataRecordFormatter.Config().createInstance();
    }

    private Ingester buildIngester(TreasureDataSender.Config senderConfig)
    {
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

        TreasureDataSender sender = senderConfig.createInstance();

        TreasureDataIngester.Config ingesterConfig = new TreasureDataIngester.Config();

        return ingesterConfig.createInstance(sender);
    }
}
