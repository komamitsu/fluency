package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PackedForwardBuffer
    extends Buffer<PackedForwardBuffer.Config>
{
    private static final int BUFFER_INITIAL_SIZE = 512 * 1024;
    private static final float BUFFER_EXPAND_RATIO = 1.5f;
    private final Map<String, ByteBuffer> messagesInTags = new HashMap<String, ByteBuffer>();
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    public PackedForwardBuffer()
    {
        this(new Config());
    }

    public PackedForwardBuffer(PackedForwardBuffer.Config bufferConfig)
    {
        super(bufferConfig);
    }

    private synchronized ByteBuffer prepareBuffer(String tag, int writeSize)
            throws BufferFullException
    {
        ByteBuffer messages = messagesInTags.get(tag);
        if (messages != null && messages.remaining() > writeSize) {
            return messages;
        }

        int origMessagesSize;
        int newMessagesSize;
        if (messages == null) {
            origMessagesSize = 0;
            newMessagesSize = BUFFER_INITIAL_SIZE;
        }
        else{
            origMessagesSize = messages.capacity();
            newMessagesSize = (int) (messages.capacity() * BUFFER_EXPAND_RATIO);
        }

        while (newMessagesSize < writeSize) {
            newMessagesSize *= BUFFER_EXPAND_RATIO;
        }

        int newTotalSize = totalSize.get() + (newMessagesSize - origMessagesSize);
        if (newTotalSize > bufferConfig.getBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", totalSize=" + newTotalSize);
        }
        ByteBuffer newMessages = ByteBuffer.allocate(newMessagesSize);
        if (messages != null) {
            newMessages.put(messages);
        }
        messagesInTags.put(tag, newMessages);
        return newMessages;
    }

    @Override
    public synchronized void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(timestamp, data));
        outputStream.close();

        ByteBuffer buffer = prepareBuffer(tag, outputStream.size());
        buffer.put(outputStream.toByteArray());
        totalSize.getAndAdd(outputStream.size());
    }

    @Override
    public synchronized void flushInternal(Sender sender)
            throws IOException
    {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        MessagePacker messagePacker = MessagePack.newDefaultPacker(header);
        for (Map.Entry<String, ByteBuffer> entry : messagesInTags.entrySet()) {
            String tag = entry.getKey();
            ByteBuffer byteBuffer = entry.getValue();
            messagePacker.packArrayHeader(2);
            messagePacker.packString(tag);
            messagePacker.packRawStringHeader(byteBuffer.position());
            messagePacker.flush();
            sender.send(ByteBuffer.wrap(header.toByteArray()));
            byteBuffer.flip();
            sender.send(byteBuffer);
        }
        // TODO: More robust
        messagesInTags.clear();
        totalSize.set(0);
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        messagesInTags.clear();
    }

    public static class Config extends Buffer.Config
    {
        private int chunkSize;

        @Override
        public int getChunkSize()
        {
            return chunkSize;
        }

        @Override
        public void setChunkSize(int chunkSize)
        {
            this.chunkSize = chunkSize;
        }
    }
}
