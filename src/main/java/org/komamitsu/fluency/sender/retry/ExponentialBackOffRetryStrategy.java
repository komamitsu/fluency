package org.komamitsu.fluency.sender.retry;

public class ExponentialBackOffRetryStrategy
    extends RetryStrategy
{
    private final Config config;

    protected ExponentialBackOffRetryStrategy(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
    }

    @Override
    public long getNextIntervalMillis(int retryCount)
    {
        long interval = config.getBaseIntervalMillis() * ((int) Math.pow(2.0, (double) retryCount));
        if (interval > config.getMaxIntervalMillis()) {
            return config.getMaxIntervalMillis();
        }
        return interval;
    }

    @Override
    public String toString()
    {
        return "ExponentialBackOffRetryStrategy{" +
                "config=" + config +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private RetryStrategy.Config baseConfig = new RetryStrategy.Config();
        private long baseIntervalMillis = 400;
        private long maxIntervalMillis = 30 * 1000;

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

        public long getBaseIntervalMillis()
        {
            return baseIntervalMillis;
        }

        public Config setBaseIntervalMillis(long baseIntervalMillis)
        {
            this.baseIntervalMillis = baseIntervalMillis;
            return this;
        }

        public long getMaxIntervalMillis()
        {
            return maxIntervalMillis;
        }

        public Config setMaxIntervalMillis(long maxIntervalMillis)
        {
            this.maxIntervalMillis = maxIntervalMillis;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", baseIntervalMillis=" + baseIntervalMillis +
                    ", maxIntervalMillis=" + maxIntervalMillis +
                    '}';
        }

        @Override
        public ExponentialBackOffRetryStrategy createInstance()
        {
            return new ExponentialBackOffRetryStrategy(this);
        }
    }
}
