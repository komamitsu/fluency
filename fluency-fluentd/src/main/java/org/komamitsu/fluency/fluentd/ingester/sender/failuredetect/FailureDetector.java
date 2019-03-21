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

package org.komamitsu.fluency.fluentd.ingester.sender.failuredetect;

import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.Min;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

public class FailureDetector
        implements Heartbeater.Callback, Closeable
{
    private final FailureDetectStrategy failureDetectStrategy;
    private final Heartbeater heartbeater;
    private final AtomicReference<Long> lastFailureTimestampMillis = new AtomicReference<>();
    private final Config config;

    public FailureDetector(FailureDetectStrategy failureDetectStrategy, Heartbeater heartbeater, Config config)
    {
        this.failureDetectStrategy = failureDetectStrategy;
        this.heartbeater = heartbeater;
        this.heartbeater.setCallback(this);
        this.heartbeater.start();
        this.config = config;
    }

    public FailureDetector(FailureDetectStrategy failureDetectStrategy, Heartbeater heartbeater)
    {
        this(failureDetectStrategy, heartbeater, new Config());
    }

    @Override
    public void onHeartbeat()
    {
        failureDetectStrategy.heartbeat(System.currentTimeMillis());
    }

    @Override
    public void onFailure(Throwable cause)
    {
        lastFailureTimestampMillis.set(System.currentTimeMillis());
    }

    public boolean isAvailable()
    {
        Long failureTimestamp = lastFailureTimestampMillis.get();
        if (failureTimestamp != null) {
            if (failureTimestamp > System.currentTimeMillis() - config.getFailureIntervalMillis()) {
                return false;
            }
            else {
                lastFailureTimestampMillis.set(null);
            }
        }
        return failureDetectStrategy.isAvailable();
    }

    @Override
    public void close()
    {
        heartbeater.close();
    }

    public FailureDetectStrategy getFailureDetectStrategy()
    {
        return failureDetectStrategy;
    }

    public Heartbeater getHeartbeater()
    {
        return heartbeater;
    }

    public int getFailureIntervalMillis()
    {
        return config.getFailureIntervalMillis();
    }

    @Override
    public String toString()
    {
        return "FailureDetector{" +
                "failureDetectStrategy=" + failureDetectStrategy +
                ", heartbeater=" + heartbeater +
                ", lastFailureTimestampMillis=" + lastFailureTimestampMillis +
                ", config=" + config +
                '}';
    }

    public static class Config
        implements Validatable
    {
        @Min(0)
        private int failureIntervalMillis = 3 * 1000;

        public int getFailureIntervalMillis()
        {
            return failureIntervalMillis;
        }

        public void setFailureIntervalMillis(int failureIntervalMillis)
        {
            this.failureIntervalMillis = failureIntervalMillis;
        }

        void validateValues()
        {
            validate();
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "failureIntervalMillis=" + failureIntervalMillis +
                    '}';
        }
    }
}
