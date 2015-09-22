package org.komamitsu.fluency.sender.failuredetect;

import org.komamitsu.fluency.sender.heartbeat.Heartbeater;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class FailureDetector
    implements Heartbeater.Callback, Closeable
{
    private final FailureDetectStrategy failureDetectStrategy;
    private final Heartbeater heartbeater;
    private final AtomicReference<Long> lastFailureTimestampMillis = new AtomicReference<Long>();

    private FailureDetector(FailureDetectStrategy failureDetectStrategy, Heartbeater heartbeater)
    {
        this.failureDetectStrategy = failureDetectStrategy;
        this.heartbeater = heartbeater;
        this.heartbeater.setCallback(this);
    }

    public FailureDetector(FailureDetectStrategy.Config failureDetectorStrategyConfig, Heartbeater.Config heartbeaterConfig)
            throws IOException
    {
        this(failureDetectorStrategyConfig.createInstance(), heartbeaterConfig.createInstance());
    }

    public FailureDetectStrategy getFailureDetectStrategy()
    {
        return failureDetectStrategy;
    }

    public Heartbeater getHeartbeater()
    {
        return heartbeater;
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
            // TODO: Make this value be configurable
            if (failureTimestamp > System.currentTimeMillis() - 3 * 1000) {
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
}
