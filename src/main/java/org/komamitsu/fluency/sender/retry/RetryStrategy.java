package org.komamitsu.fluency.sender.retry;

public abstract class RetryStrategy<C extends RetryStrategy.Config>
{
    protected final C config;

    public RetryStrategy(C config)
    {
        this.config = config;
    }

    public abstract long getNextIntervalMillis(int retryCount);

    public boolean isRetriedOver(int retryCount)
    {
        return retryCount > config.getMaxRetryCount();
    }

    public static abstract class Config<T extends RetryStrategy, C extends RetryStrategy.Config>
    {
        private int maxRetryCount = 8;

        public int getMaxRetryCount()
        {
            return maxRetryCount;
        }

        public C setMaxRetryCount(int maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
            return (C)this;
        }

        public abstract T createInstance();
    }
}
