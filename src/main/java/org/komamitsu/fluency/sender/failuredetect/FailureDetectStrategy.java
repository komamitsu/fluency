package org.komamitsu.fluency.sender.failuredetect;

public abstract class FailureDetectStrategy<C extends FailureDetectStrategy.Config>
{
    protected final C config;

    protected FailureDetectStrategy(C config)
    {
        this.config = config;
    }

    public abstract void heartbeat(long now);

    public abstract boolean isAvailable();

    public static class Config
    {
    }
}
