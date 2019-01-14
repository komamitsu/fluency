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
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.treasuredata.ingester.TreasureDataIngester;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;
import org.komamitsu.fluency.treasuredata.recordformat.TreasureDataRecordFormatter;

public class FluencyBuilder
    extends BaseFluencyBuilder
{
    private FluencyConfig ensuredConfig(FluencyConfig config)
    {
        return config == null ? new FluencyConfig() : config;
    }

    public Fluency build(String apikey, String endpoint, FluencyConfig config)
    {
        return buildInternal(createSenderConfig(config, endpoint, apikey), ensuredConfig(config));
    }

    public Fluency build(String apikey, String endpoint)
    {
        return buildInternal(createSenderConfig(null, endpoint, apikey), ensuredConfig(null));
    }

    public Fluency build(String apikey, FluencyConfig config)
    {
        return buildInternal(createSenderConfig(config, null, apikey), ensuredConfig(config));
    }

    public Fluency build(String apikey)
    {
        return buildInternal(createSenderConfig(null, null, apikey), ensuredConfig(null));
    }

    private TreasureDataSender.Config createSenderConfig(
            FluencyConfig config,
            String endpoint,
            String apikey)
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

    private Fluency buildInternal(
            TreasureDataSender.Config senderConfig,
            FluencyConfig config)
    {
        BaseFluencyBuilder.Configs configs = buildConfigs(config.baseConfig);

        Buffer.Config bufferConfig = configs.getBufferConfig();
        AsyncFlusher.Config flusherConfig = configs.getFlusherConfig();

        if (config.getSenderRetryMax() != null) {
            senderConfig.setRetryMax(config.getSenderRetryMax());
        }

        if (config.getSenderRetryIntervalMillis() != null) {
            senderConfig.setRetryIntervalMs(config.getSenderRetryIntervalMillis());
        }

        if (config.getSenderMaxRetryIntervalMillis() != null) {
            senderConfig.setMaxRetryIntervalMs(config.getSenderMaxRetryIntervalMillis());
        }

        if (config.getSenderRetryFactor() != null) {
            senderConfig.setRetryFactor(config.getSenderRetryFactor());
        }

        if (config.getErrorHandler() != null) {
            senderConfig.setErrorHandler(config.getErrorHandler());
        }

        if (config.getSenderWorkBufSize() != null) {
            senderConfig.setWorkBufSize(config.getSenderWorkBufSize());
        }

        TreasureDataSender sender = senderConfig.createInstance();

        TreasureDataIngester.Config ingesterConfig = new TreasureDataIngester.Config();

        return buildFromConfigs(
                new TreasureDataRecordFormatter.Config(),
                bufferConfig,
                flusherConfig,
                ingesterConfig.createInstance(sender)
        );
    }

    public static class FluencyConfig
    {
        private BaseFluencyBuilder.FluencyConfig baseConfig;
        private Integer senderRetryMax;
        private Integer senderRetryIntervalMillis;
        private Integer senderMaxRetryIntervalMillis;
        private Float senderRetryFactor;
        private Integer senderWorkBufSize;

        public FluencyConfig()
        {
            // Treasure Data isn't good at handling a lot of small fragmented chunk files
            this.baseConfig = new BaseFluencyBuilder.FluencyConfig()
                    .setBufferChunkRetentionTimeMillis(30 * 1000);
        }

        public Long getMaxBufferSize()
        {
            return baseConfig.getMaxBufferSize();
        }

        public FluencyConfig setMaxBufferSize(Long maxBufferSize)
        {
            baseConfig.setMaxBufferSize(maxBufferSize);
            return this;
        }

        public Integer getBufferChunkInitialSize()
        {
            return baseConfig.getBufferChunkInitialSize();
        }

        public FluencyConfig setBufferChunkInitialSize(Integer bufferChunkInitialSize)
        {
            baseConfig.setBufferChunkInitialSize(bufferChunkInitialSize);
            return this;
        }

        public Integer getBufferChunkRetentionSize()
        {
            return baseConfig.getBufferChunkRetentionSize();
        }

        public FluencyConfig setBufferChunkRetentionSize(Integer bufferChunkRetentionSize)
        {
            baseConfig.setBufferChunkRetentionSize(bufferChunkRetentionSize);
            return this;
        }

        public Integer getBufferChunkRetentionTimeMillis()
        {
            return baseConfig.getBufferChunkRetentionTimeMillis();
        }

        public FluencyConfig setBufferChunkRetentionTimeMillis(Integer bufferChunkRetentionTimeMillis)
        {
            baseConfig.setBufferChunkRetentionTimeMillis(bufferChunkRetentionTimeMillis);
            return this;
        }

        public Integer getFlushIntervalMillis()
        {
            return baseConfig.getFlushIntervalMillis();
        }

        public FluencyConfig setFlushIntervalMillis(Integer flushIntervalMillis)
        {
            baseConfig.setFlushIntervalMillis(flushIntervalMillis);
            return this;
        }

        public String getFileBackupDir()
        {
            return baseConfig.getFileBackupDir();
        }

        public FluencyConfig setFileBackupDir(String fileBackupDir)
        {
            baseConfig.setFileBackupDir(fileBackupDir);
            return this;
        }

        public Integer getWaitUntilBufferFlushed()
        {
            return baseConfig.getWaitUntilBufferFlushed();
        }

        public FluencyConfig setWaitUntilBufferFlushed(Integer waitUntilBufferFlushed)
        {
            baseConfig.setWaitUntilBufferFlushed(waitUntilBufferFlushed);
            return this;
        }

        public Integer getWaitUntilFlusherTerminated()
        {
            return baseConfig.getWaitUntilFlusherTerminated();
        }

        public FluencyConfig setWaitUntilFlusherTerminated(Integer waitUntilFlusherTerminated)
        {
            baseConfig.setWaitUntilFlusherTerminated(waitUntilFlusherTerminated);
            return this;
        }

        public Boolean getJvmHeapBufferMode()
        {
            return baseConfig.getJvmHeapBufferMode();
        }

        public FluencyConfig setJvmHeapBufferMode(Boolean jvmHeapBufferMode)
        {
            baseConfig.setJvmHeapBufferMode(jvmHeapBufferMode);
            return this;
        }

        public ErrorHandler getErrorHandler()
        {
            return baseConfig.getErrorHandler();
        }

        public FluencyConfig setErrorHandler(ErrorHandler errorHandler)
        {
            baseConfig.setErrorHandler(errorHandler);
            return this;
        }

        @Override
        public String toString()
        {
            return "FluencyConfig{" +
                    "baseConfig=" + baseConfig +
                    '}';
        }

        public Integer getSenderRetryMax()
        {
            return senderRetryMax;
        }

        public FluencyConfig setSenderRetryMax(Integer senderRetryMax)
        {
            this.senderRetryMax = senderRetryMax;
            return this;
        }

        public Integer getSenderRetryIntervalMillis()
        {
            return senderRetryIntervalMillis;
        }

        public FluencyConfig setSenderRetryIntervalMillis(Integer senderRetryIntervalMillis)
        {
            this.senderRetryIntervalMillis = senderRetryIntervalMillis;
            return this;
        }

        public Integer getSenderMaxRetryIntervalMillis()
        {
            return senderMaxRetryIntervalMillis;
        }

        public FluencyConfig setSenderMaxRetryIntervalMillis(Integer senderMaxRetryIntervalMillis)
        {
            this.senderMaxRetryIntervalMillis = senderMaxRetryIntervalMillis;
            return this;
        }

        public Float getSenderRetryFactor()
        {
            return senderRetryFactor;
        }

        public FluencyConfig setSenderRetryFactor(Float senderRetryFactor)
        {
            this.senderRetryFactor = senderRetryFactor;
            return this;
        }

        public Integer getSenderWorkBufSize()
        {
            return senderWorkBufSize;
        }

        public FluencyConfig setSenderWorkBufSize(Integer senderWorkBufSize)
        {
            this.senderWorkBufSize = senderWorkBufSize;
            return this;
        }
    }
}
