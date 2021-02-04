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

package org.komamitsu.fluency.fluentd.ingester.sender;

import org.komamitsu.fluency.fluentd.ingester.sender.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetryableSender
        extends FluentdSender
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryableSender.class);
    private final FluentdSender baseSender;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private RetryStrategy retryStrategy;

    public RetryableSender(FluentdSender baseSender, RetryStrategy retryStrategy)
    {
        this(new Config(), baseSender, retryStrategy);
    }

    public RetryableSender(Config config, FluentdSender baseSender, RetryStrategy retryStrategy)
    {
        super(config);
        this.baseSender = baseSender;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void close()
            throws IOException
    {
        baseSender.close();
        isClosed.set(true);
    }

    @Override
    public boolean isAvailable()
    {
        return true;
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> buffers, String ackToken)
            throws IOException
    {
        IOException firstException = null;

        int retry = 0;
        while (!retryStrategy.isRetriedOver(retry)) {
            if (isClosed.get()) {
                throw new RetryOverException("This sender is already closed", firstException);
            }

            try {
                if (ackToken == null) {
                    baseSender.send(buffers);
                }
                else {
                    baseSender.sendWithAck(buffers, ackToken);
                }
                return;
            }
            catch (IOException e) {
                firstException = e;
                LOG.warn("Sender failed to send data. sender=" + this + ", retry=" + retry, e);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(retryStrategy.getNextIntervalMillis(retry));
            }
            catch (InterruptedException e) {
                LOG.debug("Interrupted while waiting", e);
                Thread.currentThread().interrupt();
            }
            retry++;
        }

        throw new RetryOverException("Sending data was retried over", firstException);
    }

    public FluentdSender getBaseSender()
    {
        return baseSender;
    }

    public RetryStrategy getRetryStrategy()
    {
        return retryStrategy;
    }

    public boolean isClosed()
    {
        return isClosed.get();
    }

    @Override
    public String toString()
    {
        return "RetryableSender{" +
                "baseSender=" + baseSender +
                ", retryStrategy=" + retryStrategy +
                ", isClosed=" + isClosed +
                "} " + super.toString();
    }

    public static class RetryOverException
            extends IOException
    {
        public RetryOverException(String s, Throwable throwable)
        {
            super(s, throwable);
        }
    }

    public static class Config
            extends FluentdSender.Config
    {
    }
}
