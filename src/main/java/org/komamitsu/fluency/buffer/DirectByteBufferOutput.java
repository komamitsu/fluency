package org.komamitsu.fluency.buffer;

import org.msgpack.core.buffer.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.msgpack.core.buffer.MessageBuffer;

class DirectByteBufferOutput
        implements MessageBufferOutput
{
    private List<MessageBuffer> messageBuffers = new LinkedList<MessageBuffer>();
    private AtomicReference<MessageBuffer> currentBuffer = new AtomicReference<MessageBuffer>();

    private MessageBuffer getCurrentMessageBuffer(int bufferSize)
    {
        MessageBuffer messageBuffer;
        if ((messageBuffer = currentBuffer.get()) != null) {
            return messageBuffer;
        }
        currentBuffer.set(MessageBuffer.newDirectBuffer(bufferSize));
        return currentBuffer.get();
    }

    private void replaceCurrentBufferIfExists()
    {
        MessageBuffer messageBuffer;
        if ((messageBuffer = currentBuffer.getAndSet(null)) != null) {
            messageBuffer.getReference().limit(messageBuffer.size());
            messageBuffers.add(messageBuffer);
        }
    }

    @Override
    public MessageBuffer next(int bufferSize)
            throws IOException
    {
        replaceCurrentBufferIfExists();
        currentBuffer.set(MessageBuffer.newDirectBuffer(bufferSize));
        return currentBuffer.get();
    }

    @Override
    public void flush(MessageBuffer buf)
            throws IOException
    {
        if (buf.hasArray()) {
            byte[] src = buf.getArray();
            MessageBuffer messageBuffer = getCurrentMessageBuffer(src.length);
            int messageBufSize;
            if (messageBuffer.size() == messageBuffer.getReference().capacity() && messageBuffer.getReference().position() == 0) {
                messageBufSize = 0;
            }
            else {
                messageBufSize = messageBuffer.size();
            }

            // TODO: Consider the boundary
            messageBuffer.putBytes(messageBufSize, src, 0, src.length);
            try {
                Field size = messageBuffer.getClass().getDeclaredField("size");
                size.setAccessible(true);
                size.setInt(messageBuffer, messageBufSize + buf.size());
            }
            catch (NoSuchFieldException e) {
                throw new RuntimeException("Failed to access `size` field", e);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access `size` field", e);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        replaceCurrentBufferIfExists();
    }

    public List<MessageBuffer> getMessageBuffers()
    {
        return messageBuffers;
    }

    public int getTotalWrittenBytes()
    {
        int total = 0;
        for (MessageBuffer buffer : messageBuffers) {
            total += buffer.size();
        }
        if (currentBuffer.get() != null) {
            total += currentBuffer.get().getReference().position();
        }
        return total;
    }
}
