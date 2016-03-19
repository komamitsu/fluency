package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.BufferFullException;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageBuffer
    extends Buffer<MessageBuffer.Config>
{
    public static final String FORMAT_TYPE = "message";
    private static final Logger LOG = LoggerFactory.getLogger(MessageBuffer.class);
    private final AtomicInteger allocatedSize = new AtomicInteger();
    private final LinkedBlockingQueue<ByteBuffer> messages = new LinkedBlockingQueue<ByteBuffer>();
    private final Object bufferLock = new Object();

    private MessageBuffer(MessageBuffer.Config bufferConfig)
    {
        super(bufferConfig);
    }

    private void loadDataToMessages(ByteBuffer src)
            throws IOException
    {
        synchronized (bufferLock) {
            if (allocatedSize.get() + src.remaining() > bufferConfig.getMaxBufferSize()) {
                throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", allocatedSize=" + allocatedSize);
            }
            int position = src.position();
            messages.add(src);
            allocatedSize.getAndAdd(position - src.remaining());
        }
    }

    @Override
    public void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        byte[] packedBytes = null;
        ObjectMapper objectMapper = objectMapperHolder.get();
        ByteArrayOutputStream outputStream = outputStreamHolder.get();
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(tag, timestamp, data));
        outputStream.close();
        packedBytes = outputStream.toByteArray();

        loadDataToMessages(ByteBuffer.wrap(packedBytes));
    }

    @Override
    protected void loadBufferFromFile(List<String> params, FileChannel channel)
    {
        if (params.size() != 0) {
            throw new IllegalArgumentException("The number of params should be 0: params=" + params);
        }

        MessageUnpacker unpacker = null;
        try {
            unpacker = MessagePack.newDefaultUnpacker(channel);
            while (unpacker.hasNext()) {
                Value value = unpacker.unpackValue();
                ArrayValue arrayValue = null;
                if (!value.isArrayValue() || (arrayValue = value.asArrayValue()).size() != 3) {
                    LOG.warn("Unexpected value. Skipping it... : value={}", value);
                    continue;
                }

                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                MessagePacker packer = MessagePack.newDefaultPacker(outputStream);
                arrayValue.writeTo(packer);
                packer.close();
                loadDataToMessages(ByteBuffer.wrap(outputStream.toByteArray()));
            }
        }
        catch (Exception e) {
            LOG.error("Failed to load data to messages: params=" + params + ", channel=" + channel, e);
        }
        finally {
            if (unpacker != null) {
                try {
                    unpacker.close();
                }
                catch (IOException e) {
                    LOG.warn("Failed to close unpacker: unpacker=" + unpacker, e);
                }
            }
        }
    }

    @Override
    protected void saveAllBuffersToFile()
            throws IOException
    {
        final int bufferSize = 1024 * 1024;
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        ByteBuffer message;
        while ((message = messages.poll()) != null) {
            if (message.remaining() > bufferSize) {
                LOG.warn("This message's size is too large. Skipping it... : message={}, bufferSize={}", message, bufferSize);
                continue;
            }
            if (message.remaining() > buffer.remaining()) {
                buffer.flip();
                saveBuffer(Collections.<String>emptyList(), buffer);
                buffer.clear();
            }
            if (bufferConfig.isAckResponseMode()) {
                // Revert temporary change
                message.put(0, (byte)0x93);
            }
            buffer.put(message);
        }
        if (buffer.position() > 0) {
            buffer.flip();
            saveBuffer(new ArrayList<String>(), buffer);
        }
    }

    @Override
    public String bufferFormatType()
    {
        return FORMAT_TYPE;
    }

    @Override
    public void flushInternal(Sender sender, boolean force)
            throws IOException
    {
        ByteBuffer message;
        while ((message = messages.poll()) != null) {
            boolean keepBuffer = false;
            // TODO: Refactoring
            try {
                synchronized (bufferLock) {
                    if (bufferConfig.isAckResponseMode()) {
                        byte header = message.get(0);
                        if (header != (byte)0x93) {
                            throw new IllegalStateException("packedBytes[0] should be 0x93, but header=" + header);
                        }
                        // Temporary change
                        message.put(0, (byte)0x94);

                        String uuid = UUID.randomUUID().toString();
                        sender.sendWithAck(Arrays.asList(message), uuid.getBytes(CHARSET));
                    }
                    else {
                        sender.send(message);
                    }
                }
            }
            catch (IOException e) {
                LOG.warn("Failed to send message. The message is going to be saved into the buffer again: message={}", message);
                keepBuffer = true;
                throw e;
            }
            finally {
                if (bufferConfig.isAckResponseMode()) {
                    // Revert temporary change
                    message.put(0, (byte)0x93);
                }

                if (keepBuffer) {
                    try {
                        messages.put(message);
                    }
                    catch (InterruptedException e1) {
                        LOG.warn("Failed to save the message into the buffer: message={}", message);
                    }
                }
                else {
                    allocatedSize.addAndGet(-message.capacity());
                }
            }
        }
    }

    @Override
    protected void closeInternal()
    {
        messages.clear();
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    public static class Config extends Buffer.Config<MessageBuffer, Config>
    {
        @Override
        protected MessageBuffer createInstanceInternal()
        {
            return new MessageBuffer(this);
        }
    }
}
