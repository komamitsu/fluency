package org.komamitsu.fluency.buffer;

public class BufferConfig
{
    private final int bufferSize;

    private BufferConfig(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public static class Builder
    {
        private int bufferSize = 16 * 1024 * 1024;

        public void setBufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
        }

        public BufferConfig build()
        {
            return new BufferConfig(bufferSize);
        }
    }

    @Override
    public String toString()
    {
        return "BufferConfig{" +
                "bufferSize=" + bufferSize +
                '}';
    }
}
