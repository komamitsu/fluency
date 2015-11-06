package org.komamitsu.fluency.buffer;

import org.msgpack.core.buffer.*;

import java.io.IOException;
import java.util.List;

public class DirectByteBufferInput
        implements MessageBufferInput
{
    private final List<org.msgpack.core.buffer.MessageBuffer> messageBuffers;

    public DirectByteBufferInput(List<org.msgpack.core.buffer.MessageBuffer> messageBuffers)
    {
        this.messageBuffers = messageBuffers;
    }

    @Override
    public org.msgpack.core.buffer.MessageBuffer next()
            throws IOException
    {
        return messageBuffers.remove(0);
    }

    @Override
    public void close()
            throws IOException
    {
        messageBuffers.clear();
    }
}
