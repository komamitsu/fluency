package org.komamitsu.fluency.sender;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockTCPSender extends TCPSender
{
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final List<Object> events = new ArrayList<Object>();
    private final AtomicInteger closeCount = new AtomicInteger();

    public MockTCPSender(String host, int port)
            throws IOException
    {
        super(host, port);
    }

    public MockTCPSender(int port)
            throws IOException
    {
        super(port);
    }

    @Override
    public synchronized void send(ByteBuffer data)
            throws IOException
    {
        byte[] bytes = new byte[data.limit()];
        data.get(bytes);
        events.add(objectMapper.readValue(bytes, new TypeReference<List<Object>>() {}));
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closeCount.incrementAndGet();
    }

    public List<Object> getEvents()
    {
        return events;
    }

    public AtomicInteger getCloseCount()
    {
        return closeCount;
    }
}
