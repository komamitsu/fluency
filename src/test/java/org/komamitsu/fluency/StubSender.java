package org.komamitsu.fluency;

import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class StubSender
        extends Sender
{
    public StubSender()
    {
        super(new Sender.Config());
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
}
