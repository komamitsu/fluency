/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.sender.failuredetect;

import org.komamitsu.failuredetector.PhiAccuralFailureDetector;

public class PhiAccrualFailureDetectStrategy
        extends FailureDetectStrategy
{
    private final PhiAccuralFailureDetector failureDetector;
    private final Config config;

    protected PhiAccrualFailureDetectStrategy(Config config)
    {
        super(config.getBaseConfig());
        this.config = config;
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

    public double getPhiThreshold()
    {
        return config.getPhiThreshold();
    }

    public int getArrivalWindowSize()
    {
        return config.getArrivalWindowSize();
    }

    @Override
    public String toString()
    {
        return "PhiAccrualFailureDetectStrategy{" +
                "failureDetector=" + failureDetector +
                "} " + super.toString();
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
