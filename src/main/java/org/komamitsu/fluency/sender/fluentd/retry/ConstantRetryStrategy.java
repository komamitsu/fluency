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

package org.komamitsu.fluency.sender.fluentd.retry;

public class ConstantRetryStrategy
    extends RetryStrategy
{
    private final Config config;

    protected ConstantRetryStrategy(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
    }

    @Override
    public int getNextIntervalMillis(int retryCount)
    {
        return config.getRetryIntervalMillis();
    }

    @Override
    public String toString()
    {
        return "ConstantRetryStrategy{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
        implements Instantiator
    {
        private RetryStrategy.Config baseConfig = new RetryStrategy.Config();
        private int retryIntervalMillis = 2000;

        public RetryStrategy.Config getBaseConfig()
        {
            return baseConfig;
        }

        public int getMaxRetryCount()
        {
            return baseConfig.getMaxRetryCount();
        }

        public Config setMaxRetryCount(int maxRetryCount)
        {
            baseConfig.setMaxRetryCount(maxRetryCount);
            return this;
        }

        public int getRetryIntervalMillis()
        {
            return retryIntervalMillis;
        }

        public Config setRetryIntervalMillis(int retryIntervalMillis)
        {
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", retryIntervalMillis=" + retryIntervalMillis +
                    '}';
        }

        @Override
        public ConstantRetryStrategy createInstance()
        {
            return new ConstantRetryStrategy(this);
        }
    }
}
