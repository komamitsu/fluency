package org.komamitsu.fluency.sender.retry;

public class ConstantRetryStrategy
    extends RetryStrategy<ConstantRetryStrategy.Config>
{
    private ConstantRetryStrategy(Config config)
    {
        super(config);
    }

    @Override
    public long getNextIntervalMillis(int retryCount)
    {
        return config.getRetryIntervalMillis();
    }

    public static class Config extends RetryStrategy.Config<ConstantRetryStrategy, Config>
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

        @Override
        public ConstantRetryStrategy createInstance()
        {
            return new ConstantRetryStrategy(this);
        }
    }
}
