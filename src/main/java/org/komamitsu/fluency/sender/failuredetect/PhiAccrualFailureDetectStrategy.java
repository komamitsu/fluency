package org.komamitsu.fluency.sender.failuredetect;

import org.komamitsu.failuredetector.PhiAccuralFailureDetector;

public class PhiAccrualFailureDetectStrategy extends FailureDetectStrategy<PhiAccrualFailureDetectStrategy.Config>
{
    private final PhiAccuralFailureDetector failureDetector;

    public PhiAccrualFailureDetectStrategy(Config config)
    {
        super(config);
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

    public static class Config extends FailureDetectStrategy.Config
    {
        private double phiThreshold = 16;
        private int arrivalWindowSize = 100;

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
    }
}
