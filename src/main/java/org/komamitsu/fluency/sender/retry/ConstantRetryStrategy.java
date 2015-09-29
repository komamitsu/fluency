package org.komamitsu.fluency.sender.retry;

public class ConstantRetryStrategy
    extends RetryStrategy<ConstantRetryStrategy.Config>
{
    public ConstantRetryStrategy(Config config)
    {
        super(config);
    }

    @Override
    public long getNextIntervalMillis(int retryCount)
    {
        return config.getRetryIntervalMillis();
    }

    public static class Config extends RetryStrategy.Config<Config>
    {
        private long retryIntervalMillis = 2000;

        public long getRetryIntervalMillis()
        {
            return retryIntervalMillis;
        }

        public Config setRetryIntervalMillis(long retryIntervalMillis)
        {
            this.retryIntervalMillis = retryIntervalMillis;
            return this;
        }
    }
}
