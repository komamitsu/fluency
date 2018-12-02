/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.sender.FluentdSender;
import org.komamitsu.fluency.sender.MultiSender;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.SSLSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.SenderErrorHandler;
import org.komamitsu.fluency.sender.TCPSender;
import org.komamitsu.fluency.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.transporter.FluentdTransporter;
import org.komamitsu.fluency.transporter.Transporter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class FluencyBuilder
{
    private static Fluency buildFromConfigs(
            Buffer.Instantiator bufferConfig,
            Flusher.Instantiator flusherConfig,
            Transporter transporter)
    {
        Buffer buffer =
                (bufferConfig != null ? bufferConfig : new Buffer.Config()).
                        createInstance();

        Flusher flusher =
                (flusherConfig != null ? flusherConfig : new AsyncFlusher.Config()).
                        createInstance(buffer, transporter);

        return new Fluency(buffer, flusher);
    }

    public static class ForFluentd
    {
        public static Fluency build(String host, int port, FluencyConfig config)
        {
            return buildInternal(createBaseSenderConfig(config, host, port), config);
        }

        public static Fluency build(int port, FluencyConfig config)
        {
            return buildInternal(createBaseSenderConfig(config, null , port), config);
        }

        public static Fluency build(FluencyConfig config)
        {
            return buildInternal(createBaseSenderConfig(config, null, null), config);
        }

        public static Fluency build(List<InetSocketAddress> servers, FluencyConfig config)
        {
            List<Sender.Instantiator<? extends Sender>> senderConfigs = new ArrayList<>();
            for (InetSocketAddress server : servers) {
                senderConfigs.add(createBaseSenderConfig(config, server.getHostName(), server.getPort(), true));
            }
            return buildInternal(new MultiSender.Config(senderConfigs), config);
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
            return buildInternal(createBaseSenderConfig(null, null, null), null);
        }

        public static Fluency build(List<InetSocketAddress> servers)
        {
            return build(servers, null);
        }

        private static Sender.Instantiator<? extends Sender> createBaseSenderConfig(
                FluencyConfig config,
                String host,
                Integer port)
        {
            return createBaseSenderConfig(config, host, port, false);
        }

        private static Sender.Instantiator<? extends FluentdSender> createBaseSenderConfig(
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
                Sender.Instantiator<? extends Sender> baseSenderConfig,
                FluencyConfig config)
        {
            Buffer.Config bufferConfig = new Buffer.Config();
            ExponentialBackOffRetryStrategy.Config retryStrategyConfig = new ExponentialBackOffRetryStrategy.Config();
            AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config();
            FluentdTransporter.Config transporterConfig = new FluentdTransporter.Config();

            if (config != null) {
                if (config.getMaxBufferSize() != null) {
                    bufferConfig.setMaxBufferSize(config.getMaxBufferSize());
                }

                if (config.getBufferChunkInitialSize() != null) {
                    bufferConfig.setChunkInitialSize(config.getBufferChunkInitialSize());
                }

                if (config.getBufferChunkRetentionSize() != null) {
                    bufferConfig.setChunkRetentionSize(config.getBufferChunkRetentionSize());
                }

                if (config.getFileBackupDir() != null) {
                    bufferConfig.setFileBackupDir(config.getFileBackupDir());
                }

                if (config.getJvmHeapBufferMode() != null) {
                    bufferConfig.setJvmHeapBufferMode(config.jvmHeapBufferMode);
                }

                if (config.getFlushIntervalMillis() != null) {
                    flusherConfig.setFlushIntervalMillis(config.getFlushIntervalMillis());
                }

                if (config.getWaitUntilBufferFlushed() != null) {
                    flusherConfig.setWaitUntilBufferFlushed(config.getWaitUntilBufferFlushed());
                }

                if (config.getWaitUntilFlusherTerminated() != null) {
                    flusherConfig.setWaitUntilTerminated(config.getWaitUntilFlusherTerminated());
                }

                if (config.getSenderMaxRetryCount() != null) {
                    retryStrategyConfig.setMaxRetryCount(config.getSenderMaxRetryCount());
                }

                transporterConfig.setAckResponseMode(config.isAckResponseMode());
            }

            RetryableSender.Config senderConfig = new RetryableSender.Config(baseSenderConfig)
                    .setRetryStrategyConfig(retryStrategyConfig);

            if (config != null) {
                if (config.getSenderErrorHandler() != null) {
                    senderConfig.setSenderErrorHandler(config.getSenderErrorHandler());
                }
            }

            RetryableSender retryableSender = senderConfig.createInstance();

            return buildFromConfigs(
                    bufferConfig,
                    flusherConfig,
                    transporterConfig.createInstance(retryableSender)
            );
        }


        class FluencyConfig
        {
            private Long maxBufferSize;

            private Integer bufferChunkInitialSize;

            private Integer bufferChunkRetentionSize;

            private Integer flushIntervalMillis;

            private Integer senderMaxRetryCount;

            private boolean ackResponseMode;

            private String fileBackupDir;

            private Integer waitUntilBufferFlushed;

            private Integer waitUntilFlusherTerminated;

            private Boolean jvmHeapBufferMode;

            private SenderErrorHandler senderErrorHandler;

            private boolean sslEnabled;

            public Long getMaxBufferSize()
            {
                return maxBufferSize;
            }

            public FluencyConfig setMaxBufferSize(Long maxBufferSize)
            {
                this.maxBufferSize = maxBufferSize;
                return this;
            }

            public Integer getBufferChunkInitialSize()
            {
                return bufferChunkInitialSize;
            }

            public FluencyConfig setBufferChunkInitialSize(Integer bufferChunkInitialSize)
            {
                this.bufferChunkInitialSize = bufferChunkInitialSize;
                return this;
            }

            public Integer getBufferChunkRetentionSize()
            {
                return bufferChunkRetentionSize;
            }

            public FluencyConfig setBufferChunkRetentionSize(Integer bufferChunkRetentionSize)
            {
                this.bufferChunkRetentionSize = bufferChunkRetentionSize;
                return this;
            }

            public Integer getFlushIntervalMillis()
            {
                return flushIntervalMillis;
            }

            public FluencyConfig setFlushIntervalMillis(Integer flushIntervalMillis)
            {
                this.flushIntervalMillis = flushIntervalMillis;
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

            public String getFileBackupDir()
            {
                return fileBackupDir;
            }

            public FluencyConfig setFileBackupDir(String fileBackupDir)
            {
                this.fileBackupDir = fileBackupDir;
                return this;
            }

            public Integer getWaitUntilBufferFlushed()
            {
                return waitUntilBufferFlushed;
            }

            public FluencyConfig setWaitUntilBufferFlushed(Integer wait)
            {
                this.waitUntilBufferFlushed = wait;
                return this;
            }

            public Integer getWaitUntilFlusherTerminated()
            {
                return waitUntilFlusherTerminated;
            }

            public FluencyConfig setWaitUntilFlusherTerminated(Integer wait)
            {
                this.waitUntilFlusherTerminated = wait;
                return this;
            }

            public Boolean getJvmHeapBufferMode()
            {
                return jvmHeapBufferMode;
            }

            public FluencyConfig setJvmHeapBufferMode(Boolean jvmHeapBufferMode)
            {
                this.jvmHeapBufferMode = jvmHeapBufferMode;
                return this;
            }

            public SenderErrorHandler getSenderErrorHandler()
            {
                return senderErrorHandler;
            }

            public FluencyConfig setSenderErrorHandler(SenderErrorHandler senderErrorHandler)
            {
                this.senderErrorHandler = senderErrorHandler;
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
                return "Config{" +
                        "maxBufferSize=" + maxBufferSize +
                        ", bufferChunkInitialSize=" + bufferChunkInitialSize +
                        ", bufferChunkRetentionSize=" + bufferChunkRetentionSize +
                        ", flushIntervalMillis=" + flushIntervalMillis +
                        ", senderMaxRetryCount=" + senderMaxRetryCount +
                        ", ackResponseMode=" + ackResponseMode +
                        ", fileBackupDir='" + fileBackupDir + '\'' +
                        ", waitUntilBufferFlushed=" + waitUntilBufferFlushed +
                        ", waitUntilFlusherTerminated=" + waitUntilFlusherTerminated +
                        ", jvmHeapBufferMode=" + jvmHeapBufferMode +
                        ", senderErrorHandler=" + senderErrorHandler +
                        ", sslEnabled =" + sslEnabled +
                        '}';
            }
        }
    }
}
