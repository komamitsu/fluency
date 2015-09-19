package org.komamitsu.fluency.flusher;

public class FlusherConfig
{
    private int flushIntervalMillis = 600;
    private float bufferOccupancyThreshold = 0.6f;

    public int getFlushIntervalMillis()
    {
        return flushIntervalMillis;
    }

    public FlusherConfig setFlushIntervalMillis(int flushIntervalMillis)
    {
        this.flushIntervalMillis = flushIntervalMillis;
        return this;
    }

    public float getBufferOccupancyThreshold()
    {
        return bufferOccupancyThreshold;
    }

    public FlusherConfig setBufferOccupancyThreshold(float bufferOccupancyThreshold)
    {
        this.bufferOccupancyThreshold = bufferOccupancyThreshold;
        return this;
    }
}
