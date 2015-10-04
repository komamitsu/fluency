package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

public class MessageBuffer
    extends Buffer<MessageBuffer.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(MessageBuffer.class);
    private final LinkedBlockingQueue<ByteBuffer> messages = new LinkedBlockingQueue<ByteBuffer>();
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    private MessageBuffer(MessageBuffer.Config bufferConfig)
    {
        super(bufferConfig);
    }

    @Override
    public synchronized void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(tag, timestamp, data));
        outputStream.close();

        byte[] packedBytes = outputStream.toByteArray();
        if (bufferConfig.isAckResponseMode()) {
            if (packedBytes[0] != (byte)0x93) {
                throw new IllegalStateException("packedBytes[0] should be 0x93, but " + packedBytes[0]);
            }
            packedBytes[0] = (byte)0x94;
        }

        if (allocatedSize.get() + packedBytes.length > bufferConfig.getMaxBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", allocatedSize=" + allocatedSize);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(packedBytes);
        messages.add(byteBuffer);
        allocatedSize.getAndAdd(packedBytes.length);
    }

    @Override
    public synchronized void flushInternal(Sender sender, boolean force)
            throws IOException
    {
        ByteBuffer message = null;
        while ((message = messages.poll()) != null) {
            try {
                allocatedSize.addAndGet(-message.capacity());
                if (bufferConfig.isAckResponseMode()) {
                    String uuid = UUID.randomUUID().toString();
                    sender.sendWithAck(Arrays.asList(message), uuid);
                }
                else {
                    sender.send(message);
                }
            }
            catch (Throwable e) {
                try {
                    messages.put(message);
                    allocatedSize.addAndGet(message.capacity());
                }
                catch (InterruptedException e1) {
                    LOG.error("Interrupted during restoring fetched message. It can be lost. message={}", message);
                }

                if (e instanceof IOException) {
                    throw (IOException)e;
                }
                else {
                    throw new RuntimeException("Failed to send message to fluentd", e);
                }
            }
        }
    }

    @Override
    public void closeInternal(Sender sender)
            throws IOException
    {
        messages.clear();
    }

    public static class Config extends Buffer.Config<MessageBuffer, Config>
    {
        @Override
        public MessageBuffer createInstance()
        {
            return new MessageBuffer(this);
        }
    }
}
