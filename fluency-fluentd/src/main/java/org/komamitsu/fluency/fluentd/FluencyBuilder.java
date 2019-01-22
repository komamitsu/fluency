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
import org.komamitsu.fluency.fluentd.ingester.FluentdIngester;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.MultiSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.ingester.sender.SSLSender;
import org.komamitsu.fluency.fluentd.ingester.sender.TCPSender;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.fluentd.recordformat.FluentdRecordFormatter;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class FluencyBuilder
        extends BaseFluencyBuilder
{
    private Integer senderMaxRetryCount;
    private boolean ackResponseMode;
    private boolean sslEnabled;

    public Integer getSenderMaxRetryCount()
    {
        return senderMaxRetryCount;
    }

    public void setSenderMaxRetryCount(Integer senderMaxRetryCount)
    {
        this.senderMaxRetryCount = senderMaxRetryCount;
    }

    public boolean isAckResponseMode()
    {
        return ackResponseMode;
    }

    public void setAckResponseMode(boolean ackResponseMode)
    {
        this.ackResponseMode = ackResponseMode;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
    }

    public Fluency build(String host, int port)
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createBaseSender(host, port)));
    }

    public Fluency build(int port)
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createBaseSender(null , port)));
    }

    public Fluency build()
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createBaseSender(null, null)));
    }

    public Fluency build(List<InetSocketAddress> servers)
    {
        List<FluentdSender> senders = new ArrayList<>();
        for (InetSocketAddress server : servers) {
            senders.add(createBaseSender(server.getHostName(), server.getPort(), true));
        }
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(new MultiSender(new MultiSender.Config(), senders)));
    }

    private FluentdSender createBaseSender(String host, Integer port)
    {
        return createBaseSender(host, port, false);
    }

    private FluentdSender createBaseSender(String host, Integer port, boolean withHeartBeater)
    {
        if (withHeartBeater && port == null) {
            throw new IllegalArgumentException("`port` should be specified when using heartbeat");
        }

        if (isSslEnabled()) {
            SSLSender.Config senderConfig = new SSLSender.Config();
            FailureDetector failureDetector = null;
            if (host != null) {
                senderConfig.setHost(host);
            }
            if (port != null) {
                senderConfig.setPort(port);
            }
            if (withHeartBeater) {
                SSLHeartbeater.Config hbConfig = new SSLHeartbeater.Config();
                hbConfig.setHost(host);
                hbConfig.setPort(port);
                SSLHeartbeater heartbeater = new SSLHeartbeater(hbConfig);
                failureDetector = new FailureDetector(new PhiAccrualFailureDetectStrategy(), heartbeater);
            }
            return new SSLSender(senderConfig, failureDetector);
        }
        else {
            TCPSender.Config senderConfig = new TCPSender.Config();
            FailureDetector failureDetector = null;
            if (host != null) {
                senderConfig.setHost(host);
            }
            if (port != null) {
                senderConfig.setPort(port);
            }
            if (withHeartBeater) {
                TCPHeartbeater.Config hbConfig = new TCPHeartbeater.Config();
                hbConfig.setHost(host);
                hbConfig.setPort(port);
                TCPHeartbeater heartbeater = new TCPHeartbeater(hbConfig);
                failureDetector = new FailureDetector(new PhiAccrualFailureDetectStrategy(), heartbeater);
            }
            return new TCPSender(senderConfig, failureDetector);
        }
    }

    @Override
    public String toString()
    {
        return "FluencyBuilder{" +
                "senderMaxRetryCount=" + senderMaxRetryCount +
                ", ackResponseMode=" + ackResponseMode +
                ", sslEnabled=" + sslEnabled +
                "} " + super.toString();
    }

    private RecordFormatter buildRecordFormatter()
    {
        return new FluentdRecordFormatter(new FluentdRecordFormatter.Config());
    }

    private Ingester buildIngester(FluentdSender baseSender)
    {
        ExponentialBackOffRetryStrategy.Config retryStrategyConfig =
                new ExponentialBackOffRetryStrategy.Config();

        if (getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(getSenderMaxRetryCount());
        }

        if (getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(getSenderMaxRetryCount());
        }

        RetryableSender.Config senderConfig = new RetryableSender.Config();

        if (getErrorHandler() != null) {
            senderConfig.setErrorHandler(getErrorHandler());
        }

        RetryableSender retryableSender = new RetryableSender(senderConfig, baseSender,
                new ExponentialBackOffRetryStrategy(retryStrategyConfig));

        FluentdIngester.Config ingesterConfig = new FluentdIngester.Config();
        ingesterConfig.setAckResponseMode(isAckResponseMode());

        return new FluentdIngester(ingesterConfig, retryableSender);
    }
}
