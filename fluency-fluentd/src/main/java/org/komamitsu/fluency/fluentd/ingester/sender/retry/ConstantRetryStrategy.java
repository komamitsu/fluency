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

public class ConstantRetryStrategy
        extends RetryStrategy
{
    private final Config config;

    public ConstantRetryStrategy(Config config)
    {
        super(config);
        config.validateValues();
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
            extends RetryStrategy.Config
            implements Validatable
    {
        @Min(10)
        private int retryIntervalMillis = 2000;

        public int getRetryIntervalMillis()
        {
            return retryIntervalMillis;
        }

        public void setRetryIntervalMillis(int retryIntervalMillis)
        {
            this.retryIntervalMillis = retryIntervalMillis;
        }

        void validateValues()
        {
            validate();
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "retryIntervalMillis=" + retryIntervalMillis +
                    "} " + super.toString();
        }
    }
}
