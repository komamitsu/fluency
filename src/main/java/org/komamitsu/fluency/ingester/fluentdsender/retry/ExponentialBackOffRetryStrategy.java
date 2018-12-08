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

package org.komamitsu.fluency.ingester.fluentdsender.retry;

public class ExponentialBackOffRetryStrategy
    extends RetryStrategy
{
    private final Config config;

    protected ExponentialBackOffRetryStrategy(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
    }

    @Override
    public int getNextIntervalMillis(int retryCount)
    {
        int interval = config.getBaseIntervalMillis() * ((int) Math.pow(2.0, (double) retryCount));
        if (interval > config.getMaxIntervalMillis()) {
            return config.getMaxIntervalMillis();
        }
        return interval;
    }

    public int getBaseIntervalMillis()
    {
        return config.getBaseIntervalMillis();
    }

    public int getMaxIntervalMillis()
    {
        return config.getMaxIntervalMillis();
    }

    @Override
    public String toString()
    {
        return "ExponentialBackOffRetryStrategy{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private RetryStrategy.Config baseConfig = new RetryStrategy.Config();
        private int baseIntervalMillis = 400;
        private int maxIntervalMillis = 30 * 1000;

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

        public int getBaseIntervalMillis()
        {
            return baseIntervalMillis;
        }

        public Config setBaseIntervalMillis(int baseIntervalMillis)
        {
            this.baseIntervalMillis = baseIntervalMillis;
            return this;
        }

        public int getMaxIntervalMillis()
        {
            return maxIntervalMillis;
        }

        public Config setMaxIntervalMillis(int maxIntervalMillis)
        {
            this.maxIntervalMillis = maxIntervalMillis;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", baseIntervalMillis=" + baseIntervalMillis +
                    ", maxIntervalMillis=" + maxIntervalMillis +
                    '}';
        }

        @Override
        public ExponentialBackOffRetryStrategy createInstance()
        {
            return new ExponentialBackOffRetryStrategy(this);
        }
    }
}
