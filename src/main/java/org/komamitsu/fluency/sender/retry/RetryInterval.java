package org.komamitsu.fluency.sender.retry;

public class RetryInterval
{
    private final Integer maxRetryCount;
    private final int retryIntervalMillis;
    private final Integer maxInterval;

    public Integer getMaxRetryCount()
    {
        return maxRetryCount;
    }

    public int getRetryIntervalMillis()
    {
        return retryIntervalMillis;
    }

    public Integer getMaxInterval()
    {
        return maxInterval;
    }

    private RetryInterval(Integer maxRetryCount, int retryIntervalMillis, Integer maxInterval)
    {
        this.maxRetryCount = maxRetryCount;
        this.retryIntervalMillis = retryIntervalMillis;
        this.maxInterval = maxInterval;
    }

    public static class Builder
    {
        private Integer maxRetryCount;
        private int retryIntervalMillis = 2 * 1000;
        private Integer maxInterval = 60 * 1000;

        public Builder setMaxRetryCount(Integer maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        public Builder setRetryIntervalMillis(int retryIntervalMillis)
        {
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }

        public Builder setMaxInterval(Integer maxInterval)
        {
            this.maxInterval = maxInterval;
            return this;
        }

        public RetryInterval build()
        {
            return new RetryInterval(maxRetryCount, retryIntervalMillis, maxInterval);
        }
    }

    public boolean isRetriedOver(int retryCount)
    {
        return maxRetryCount != null && retryCount > maxRetryCount;
    }

    public long getNextIntervalMillis(long originalInterval)
    {
        if (maxInterval != null && maxInterval < originalInterval) {
            return maxInterval;
        }
        return originalInterval;
    }
}
