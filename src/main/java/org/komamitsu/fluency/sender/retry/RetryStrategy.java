package org.komamitsu.fluency.sender.retry;

public abstract class RetryStrategy<T extends RetryStrategy.Config>
{
    protected final T config;

    public RetryStrategy(T config)
    {
        this.config = config;
    }

    public abstract long getNextIntervalMillis(int retryCount);

    public boolean isRetriedOver(int retryCount)
    {
        return retryCount > config.getMaxRetryCount();
    }

    public static abstract class Config<T extends RetryStrategy.Config>
    {
        private int maxRetryCount = 8;

        public int getMaxRetryCount()
        {
            return maxRetryCount;
        }

        public T setMaxRetryCount(int maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
            return (T)this;
        }
    }
}
