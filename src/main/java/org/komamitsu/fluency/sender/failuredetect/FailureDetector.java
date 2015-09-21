package org.komamitsu.fluency.sender.failuredetect;

import org.komamitsu.fluency.sender.heartbeat.Heartbeater;

import java.io.Closeable;
import java.io.IOException;

public class FailureDetector
    implements Heartbeater.Callback, Closeable
{
    private final FailureDetectStrategy failureDetectStrategy;
    private final Heartbeater heartbeater;

    public FailureDetector(FailureDetectStrategy failureDetectStrategy, Heartbeater.Factory heartbeaterFactory, Heartbeater.Config heartbeaterConfig)
            throws IOException
    {
        this.failureDetectStrategy = failureDetectStrategy;
        heartbeater = heartbeaterFactory.create(heartbeaterConfig);
        heartbeater.setCallback(this);
    }

    @Override
    public void onHeartbeat()
    {
        failureDetectStrategy.heartbeat(System.currentTimeMillis());
    }

    public boolean isAvailable()
    {
        return failureDetectStrategy.isAvailable();
    }

    @Override
    public void close()
            throws IOException
    {
        heartbeater.close();
    }
}
