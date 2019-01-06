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

package org.komamitsu.fluency.fluentd;

import org.komamitsu.fluency.BaseFluencyBuilder;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.fluentd.ingester.FluentdIngester;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.MultiSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.ingester.sender.SSLSender;
import org.komamitsu.fluency.fluentd.ingester.sender.TCPSender;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.fluentd.recordformat.FluentdRecordFormatter;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class FluencyBuilder
{
    private static FluencyConfig ensuredConfig(FluencyConfig config)
    {
        return config == null ? new FluencyConfig() : config;
    }

    public static Fluency build(String host, int port, FluencyConfig config)
    {
        return buildInternal(createBaseSenderConfig(config, host, port), ensuredConfig(config));
    }

    public static Fluency build(int port, FluencyConfig config)
    {
        return buildInternal(createBaseSenderConfig(config, null , port), ensuredConfig(config));
    }

    public static Fluency build(FluencyConfig config)
    {
        return buildInternal(createBaseSenderConfig(config, null, null), ensuredConfig(config));
    }

    public static Fluency build(List<InetSocketAddress> servers, FluencyConfig config)
    {
        FluencyConfig ensuredConfig = ensuredConfig(config);
        List<FluentdSender.Instantiator> senderConfigs = new ArrayList<>();
        for (InetSocketAddress server : servers) {
            senderConfigs.add(createBaseSenderConfig(ensuredConfig, server.getHostName(), server.getPort(), true));
        }
        return buildInternal(new MultiSender.Config(senderConfigs), ensuredConfig);
    }

    public static Fluency build(String host, int port)
    {
        return build(host, port, null);
    }

    public static Fluency build(int port)
    {
        return build(port, null);
    }

    public static Fluency build()
    {
        return buildInternal(createBaseSenderConfig(null, null, null), ensuredConfig(null));
    }

    public static Fluency build(List<InetSocketAddress> servers)
    {
        return build(servers, null);
    }

    private static FluentdSender.Instantiator createBaseSenderConfig(
            FluencyConfig config,
            String host,
            Integer port)
    {
        return createBaseSenderConfig(config, host, port, false);
    }

    private static FluentdSender.Instantiator createBaseSenderConfig(
            FluencyConfig config,
            String host,
            Integer port,
            boolean withHeartBeater)
    {
        if (withHeartBeater && port == null) {
            throw new IllegalArgumentException("`port` should be specified when using heartbeat");
        }

        if (config != null && config.sslEnabled) {
            SSLSender.Config senderConfig = new SSLSender.Config();
            if (host != null) {
                senderConfig.setHost(host);
            }
            if (port != null) {
                senderConfig.setPort(port);
            }
            if (withHeartBeater) {
                senderConfig.setHeartbeaterConfig(
                        new SSLHeartbeater.Config()
                                .setHost(host)
                                .setPort(port));
            }
            return senderConfig;
        }
        else {
            TCPSender.Config senderConfig = new TCPSender.Config();
            if (host != null) {
                senderConfig.setHost(host);
            }
            if (port != null) {
                senderConfig.setPort(port);
            }
            if (withHeartBeater) {
                senderConfig.setHeartbeaterConfig(
                        new TCPHeartbeater.Config()
                                .setHost(host)
                                .setPort(port));
            }
            return senderConfig;
        }
    }

    private static Fluency buildInternal(
            FluentdSender.Instantiator baseSenderConfig,
            FluencyConfig config)
    {
        BaseFluencyBuilder.Configs configs = BaseFluencyBuilder.buildConfigs(config.baseConfig);

        Buffer.Config bufferConfig = configs.getBufferConfig();
        AsyncFlusher.Config flusherConfig = configs.getFlusherConfig();

        ExponentialBackOffRetryStrategy.Config retryStrategyConfig = new ExponentialBackOffRetryStrategy.Config();
        if (config.getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(config.getSenderMaxRetryCount());
        }

        if (config.getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(config.getSenderMaxRetryCount());
        }

        FluentdIngester.Config transporterConfig = new FluentdIngester.Config();
            transporterConfig.setAckResponseMode(config.isAckResponseMode());

        RetryableSender.Config senderConfig = new RetryableSender.Config(baseSenderConfig)
                .setRetryStrategyConfig(retryStrategyConfig);

        if (config.getErrorHandler() != null) {
            senderConfig.setErrorHandler(config.getErrorHandler());
        }

        RetryableSender retryableSender = senderConfig.createInstance();

        return BaseFluencyBuilder.buildFromConfigs(
                new FluentdRecordFormatter.Config(),
                bufferConfig,
                flusherConfig,
                transporterConfig.createInstance(retryableSender)
        );
    }

    public static class FluencyConfig
    {
        private BaseFluencyBuilder.FluencyConfig baseConfig = new BaseFluencyBuilder.FluencyConfig();

        private Integer senderMaxRetryCount;

        private boolean ackResponseMode;

        private boolean sslEnabled;

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

        public Integer getSenderMaxRetryCount()
        {
            return senderMaxRetryCount;
        }

        public FluencyConfig setSenderMaxRetryCount(Integer senderMaxRetryCount)
        {
            this.senderMaxRetryCount = senderMaxRetryCount;
            return this;
        }

        public boolean isAckResponseMode()
        {
            return ackResponseMode;
        }

        public FluencyConfig setAckResponseMode(boolean ackResponseMode)
        {
            this.ackResponseMode = ackResponseMode;
            return this;
        }

        public boolean isSslEnabled()
        {
            return sslEnabled;
        }

        public FluencyConfig setSslEnabled(boolean sslEnabled)
        {
            this.sslEnabled = sslEnabled;
            return this;
        }

        @Override
        public String toString()
        {
            return "FluencyConfig{" +
                    "baseConfig=" + baseConfig +
                    ", senderMaxRetryCount=" + senderMaxRetryCount +
                    ", ackResponseMode=" + ackResponseMode +
                    ", sslEnabled=" + sslEnabled +
                    '}';
        }
    }
}
