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

package org.komamitsu.fluency.sender.fluentd.failuredetect;

import org.komamitsu.fluency.sender.fluentd.heartbeat.Heartbeater;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class FailureDetector
    implements Heartbeater.Callback, Closeable
{
    private final FailureDetectStrategy failureDetectStrategy;
    private final Heartbeater heartbeater;
    private final AtomicReference<Long> lastFailureTimestampMillis = new AtomicReference<Long>();
    private final Config config;

    public FailureDetector(FailureDetectStrategy failureDetectStrategy, Heartbeater heartbeater, Config config)
    {
        this.failureDetectStrategy = failureDetectStrategy;
        this.heartbeater = heartbeater;
        this.heartbeater.setCallback(this);
        this.heartbeater.start();
        this.config = config;
    }

    private FailureDetector(FailureDetectStrategy failureDetectStrategy, Heartbeater heartbeater)
    {
        this(failureDetectStrategy, heartbeater, new Config());
    }

    public FailureDetector(FailureDetectStrategy.Instantiator failureDetectorStrategyConfig, Heartbeater.Instantiator heartbeaterConfig, Config config)
            throws IOException
    {
        this(failureDetectorStrategyConfig.createInstance(), heartbeaterConfig.createInstance(), config);
    }

    public FailureDetector(FailureDetectStrategy.Instantiator failureDetectorStrategyConfig, Heartbeater.Instantiator heartbeaterConfig)
            throws IOException
    {
        this(failureDetectorStrategyConfig.createInstance(), heartbeaterConfig.createInstance());
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
            throws IOException
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
    {
        private int failureIntervalMillis = 3 * 1000;

        public int getFailureIntervalMillis()
        {
            return failureIntervalMillis;
        }

        public void setFailureIntervalMillis(int failureIntervalMillis)
        {
            this.failureIntervalMillis = failureIntervalMillis;
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
