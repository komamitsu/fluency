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

package org.komamitsu.fluency.fluentd.ingester.sender.retry;

import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.Min;

public class ExponentialBackOffRetryStrategy
        extends RetryStrategy
{
    private final Config config;

    public ExponentialBackOffRetryStrategy()
    {
        this(new Config());
    }

    public ExponentialBackOffRetryStrategy(Config config)
    {
        super(config);
        config.validateValues();
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
            extends RetryStrategy.Config
            implements Validatable
    {
        @Min(10)
        private int baseIntervalMillis = 400;
        @Min(10)
        private int maxIntervalMillis = 30 * 1000;

        public int getBaseIntervalMillis()
        {
            return baseIntervalMillis;
        }

        public void setBaseIntervalMillis(int baseIntervalMillis)
        {
            this.baseIntervalMillis = baseIntervalMillis;
        }

        public int getMaxIntervalMillis()
        {
            return maxIntervalMillis;
        }

        public void setMaxIntervalMillis(int maxIntervalMillis)
        {
            this.maxIntervalMillis = maxIntervalMillis;
        }

        void validateValues()
        {
            validate();
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseIntervalMillis=" + baseIntervalMillis +
                    ", maxIntervalMillis=" + maxIntervalMillis +
                    "} " + super.toString();
        }
    }
}
