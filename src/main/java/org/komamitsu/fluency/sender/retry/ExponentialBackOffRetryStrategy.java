package org.komamitsu.fluency.sender.retry;

public class ExponentialBackOffRetryStrategy
    extends RetryStrategy
{
    @Override
    long getOriginalNextIntervalMillis(RetryInterval retryInterval, int retryCount)
    {
        return retryInterval.getRetryIntervalMillis() * ((int) Math.pow(2.0, (double)retryCount));
    }
}
