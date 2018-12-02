/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.StubSender;
import org.komamitsu.fluency.sender.fluentd.retry.ExponentialBackOffRetryStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RetryableSenderTest
{
    static class FailurableSender
        extends StubSender
    {
        private final int maxFailures;
        private int retry;

        public FailurableSender(int maxFailures)
        {
            this.maxFailures = maxFailures;
        }

        @Override
        protected void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
                throws IOException
        {
            if (retry < maxFailures) {
                retry++;
                throw new IOException("Something is wrong...");
            }
        }

        public int getRetry()
        {
            return retry;
        }

        static class Config
            implements Sender.Instantiator
        {
            private final int maxFailures;

            Config(int maxFailures)
            {
                this.maxFailures = maxFailures;
            }

            @Override
            public Sender createInstance()
            {
                return new FailurableSender(maxFailures);
            }
        }
    }

    @Test
    public void testSend()
            throws IOException
    {
        ExponentialBackOffRetryStrategy.Config retryStrategyConfig =
                new ExponentialBackOffRetryStrategy.Config().setMaxRetryCount(3);

        RetryableSender sender =
                new RetryableSender
                        .Config(new FailurableSender.Config(3))
                        .setRetryStrategyConfig(retryStrategyConfig)
                        .createInstance();

        FailurableSender baseSender = (FailurableSender) sender.getBaseSender();
        assertThat(baseSender.getRetry(), is(0));
        sender.send(ByteBuffer.allocate(64));
        assertThat(baseSender.getRetry(), is(3));
    }

    @Test(expected = RetryableSender.RetryOverException.class)
    public void testSendRetryOver()
            throws IOException
    {
        ExponentialBackOffRetryStrategy.Config retryStrategyConfig =
                new ExponentialBackOffRetryStrategy.Config().setMaxRetryCount(2);

        RetryableSender sender =
                new RetryableSender
                        .Config(new FailurableSender.Config(3))
                        .setRetryStrategyConfig(retryStrategyConfig)
                        .createInstance();

        FailurableSender baseSender = (FailurableSender) sender.getBaseSender();
        assertThat(baseSender.getRetry(), is(0));
        sender.send(ByteBuffer.allocate(64));
    }
}