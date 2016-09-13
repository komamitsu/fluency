package org.komamitsu.fluency.sender.retry;

public abstract class RetryStrategy
{
    private final Config config;

    protected RetryStrategy(Config config)
    {
        this.config = config;
    }

    public abstract int getNextIntervalMillis(int retryCount);

    public boolean isRetriedOver(int retryCount)
    {
        return retryCount > config.getMaxRetryCount();
    }

    public int getMaxRetryCount()
    {
        return config.getMaxRetryCount();
    }

    @Override
    public String toString()
    {
        return "RetryStrategy{" +
                "config=" + config +
                '}';
    }

    public static class Config
    {
        private int maxRetryCount = 7;

        public int getMaxRetryCount()
        {
            return maxRetryCount;
        }

        public Config setMaxRetryCount(int maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "maxRetryCount=" + maxRetryCount +
                    '}';
        }
    }

    public interface Instantiator
    {
        RetryStrategy createInstance();
    }
}
