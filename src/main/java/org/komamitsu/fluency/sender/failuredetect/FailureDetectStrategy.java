package org.komamitsu.fluency.sender.failuredetect;

public abstract class FailureDetectStrategy
{
    protected final Config config;

    protected FailureDetectStrategy(Config config)
    {
        this.config = config;
    }

    public abstract void heartbeat(long now);

    public abstract boolean isAvailable();

    public abstract static class Config
    {
        public abstract FailureDetectStrategy createInstance();
    }
}
