package org.komamitsu.fluency;

import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class StubSender
        extends Sender<StubSender.Config>
{
    public StubSender()
    {
        super(null);
    }

    @Override
    public boolean isAvailable()
    {
        return true;
    }

    @Override
    protected void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public static class Config
            extends Sender.Config<StubSender, Config>
    {
        // Dummy
        @Override
        public StubSender createInstance()
        {
            return null;
        }
    }
}
