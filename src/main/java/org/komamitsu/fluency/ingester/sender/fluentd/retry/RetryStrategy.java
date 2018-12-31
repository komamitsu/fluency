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

package org.komamitsu.fluency.ingester.sender.fluentd.retry;

public abstract class RetryStrategy
{
    private final Config config;

    protected RetryStrategy(Config config)
    {
        this.config = config;
    }

    public abstract int getNextIntervalMillis(int retryCount);

    public boolean isRetriedOver(int retryCount)
    {
        return retryCount > config.getMaxRetryCount();
    }

    public int getMaxRetryCount()
    {
        return config.getMaxRetryCount();
    }

    @Override
    public String toString()
    {
        return "RetryStrategy{" +
                "config=" + config +
                '}';
    }

    public static class Config
    {
        private int maxRetryCount = 7;

        public int getMaxRetryCount()
        {
            return maxRetryCount;
        }

        public Config setMaxRetryCount(int maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "maxRetryCount=" + maxRetryCount +
                    '}';
        }
    }

    public interface Instantiator
    {
        RetryStrategy createInstance();
    }
}
