package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageBuffer
    extends Buffer
{
    private final LinkedBlockingQueue<ByteBuffer> messages = new LinkedBlockingQueue<ByteBuffer>();
    private final AtomicInteger totalSize = new AtomicInteger();
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    public MessageBuffer()
    {
        this(new BufferConfig.Builder().build());
    }

    public MessageBuffer(BufferConfig bufferConfig)
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

        if (totalSize.get() + outputStream.size() > bufferConfig.getBufferSize()) {
            throw new BufferFullException("Buffer is full. bufferConfig=" + bufferConfig + ", totalSize=" + totalSize + ", byteBuffer=" + data);
        }

        messages.add(ByteBuffer.wrap(outputStream.toByteArray()));
        totalSize.getAndAdd(outputStream.size());
    }

    @Override
    public void flush(Sender sender)
            throws IOException
    {
        ByteBuffer message = null;
        while ((message = messages.poll()) != null) {
            sender.send(message);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        messages.clear();
    }
}
