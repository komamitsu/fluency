package org.komamitsu.fluency.sender.failuredetect;

import org.komamitsu.failuredetector.PhiAccuralFailureDetector;

public class PhiAccrualFailureDetectStrategy
        extends FailureDetectStrategy
{
    private final PhiAccuralFailureDetector failureDetector;

    protected PhiAccrualFailureDetectStrategy(Config config)
    {
        super(config.getBaseConfig());
        failureDetector = new PhiAccuralFailureDetector.Builder().
                setThreshold(config.getPhiThreshold()).
                setMaxSampleSize(config.getArrivalWindowSize()).
                build();
    }

    @Override
    public void heartbeat(long now)
    {
        failureDetector.heartbeat(now);
    }

    @Override
    public boolean isAvailable()
    {
        return failureDetector.isAvailable();
    }

    public static class Config
            implements Instantiator
    {
        private FailureDetectStrategy.Config baseConfig = new FailureDetectStrategy.Config();
        private double phiThreshold = 16;
        private int arrivalWindowSize = 100;

        public FailureDetectStrategy.Config getBaseConfig()
        {
            return baseConfig;
        }

        public double getPhiThreshold()
        {
            return phiThreshold;
        }

        public Config setPhiThreshold(float phiThreshold)
        {
            this.phiThreshold = phiThreshold;
            return this;
        }

        public int getArrivalWindowSize()
        {
            return arrivalWindowSize;
        }

        public Config setArrivalWindowSize(int arrivalWindowSize)
        {
            this.arrivalWindowSize = arrivalWindowSize;
            return this;
        }

        @Override
        public FailureDetectStrategy createInstance()
        {
            return new PhiAccrualFailureDetectStrategy(this);
        }
    }
}
