package org.komamitsu.fluency.flusher;

public class FlusherConfig
{
    private final int flushIntervalMillis;
    private final float bufferOccupancyThreshold;

    private FlusherConfig(int flushIntervalMillis, float bufferOccupancyThreshold)
    {
        this.flushIntervalMillis = flushIntervalMillis;
        this.bufferOccupancyThreshold = bufferOccupancyThreshold;
    }

    public int getFlushIntervalMillis()
    {
        return flushIntervalMillis;
    }

    public float getBufferOccupancyThreshold()
    {
        return bufferOccupancyThreshold;
    }
    
    public static class Builder
    {
        private int flushIntervalMillis = 600;
        private float bufferOccupancyThreshold = 0.6f;

        public void setFlushIntervalMillis(int flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
        }

        public void setBufferOccupancyThreshold(float bufferOccupancyThreshold)
        {
            this.bufferOccupancyThreshold = bufferOccupancyThreshold;
        }
        
        public FlusherConfig build()
        {
            return new FlusherConfig(flushIntervalMillis, bufferOccupancyThreshold);
        }
    }
}
