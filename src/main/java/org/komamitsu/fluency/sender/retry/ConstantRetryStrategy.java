package org.komamitsu.fluency.sender.retry;

public class ConstantRetryStrategy
    extends RetryStrategy
{
    private final Config config;

    private ConstantRetryStrategy(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
    }

    @Override
    public long getNextIntervalMillis(int retryCount)
    {
        return config.getRetryIntervalMillis();
    }

    public static class Config
        implements Instantiator
    {
        private RetryStrategy.Config baseConfig = new RetryStrategy.Config();
        private long retryIntervalMillis = 2000;

        public RetryStrategy.Config getBaseConfig()
        {
            return baseConfig;
        }

        public int getMaxRetryCount()
        {
            return baseConfig.getMaxRetryCount();
        }

        public Config setMaxRetryCount(int maxRetryCount)
        {
            baseConfig.setMaxRetryCount(maxRetryCount);
            return this;
        }

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
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", retryIntervalMillis=" + retryIntervalMillis +
                    '}';
        }

        @Override
        public ConstantRetryStrategy createInstance()
        {
            return new ConstantRetryStrategy(this);
        }
    }
}
