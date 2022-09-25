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

package org.komamitsu.fluency.fluentd.ingester.sender.heartbeat;

import org.komamitsu.fluency.util.ExecutorServiceUtils;
import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Heartbeater
        implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Heartbeater.class);
    private final Config config;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<Callback> callback = new AtomicReference<>();

    protected Heartbeater(Config config)
    {
        config.validateValues();
        this.config = config;
        executorService = ExecutorServiceUtils.newScheduledDaemonThreadPool(1);
    }

    public void start()
    {
        executorService.scheduleAtFixedRate(
                this::ping,
                config.getIntervalMillis(),
                config.getIntervalMillis(),
                TimeUnit.MILLISECONDS);
    }

    protected abstract void invoke()
            throws IOException;

    protected void ping()
    {
        try {
            invoke();
        }
        catch (Throwable e) {
            LOG.warn("ping(): failed, config=" + config);
            Callback callback = this.callback.get();
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }

    protected void pong()
    {
        Callback callback = this.callback.get();
        if (callback != null) {
            callback.onHeartbeat();
        }
    }

    public void setCallback(Callback callback)
    {
        this.callback.set(callback);
    }

    @Override
    public void close()
    {
        ExecutorServiceUtils.finishExecutorService(executorService);
    }

    public int getIntervalMillis()
    {
        return config.getIntervalMillis();
    }

    @Override
    public String toString()
    {
        return "Heartbeater{" +
                "config=" + config +
                '}';
    }

    public interface Callback
    {
        void onHeartbeat();

        void onFailure(Throwable cause);
    }

    public static class Config
            implements Validatable
    {
        @Min(100)
        private int intervalMillis = 1000;

        public int getIntervalMillis()
        {
            return intervalMillis;
        }

        public void setIntervalMillis(int intervalMillis)
        {
            this.intervalMillis = intervalMillis;
        }

        void validateValues()
        {
            validate();
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "intervalMillis=" + intervalMillis +
                    '}';
        }
    }
}
