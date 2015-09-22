package org.komamitsu.fluency.sender.retry;

public class ConstantRetryStrategy
    extends RetryStrategy
{
    @Override
    long getOriginalNextIntervalMillis(RetryInterval retryInterval, int retryCount)
    {
        return retryInterval.getRetryIntervalMillis();
    }
}
