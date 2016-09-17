package org.komamitsu.fluency.sender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockTCPSender
        extends TCPSender
{
    private final List<ByteBuffer> events = new ArrayList<ByteBuffer>();
    private final AtomicInteger closeCount = new AtomicInteger();

    public MockTCPSender(String host, int port)
            throws IOException
    {
        super(new TCPSender.Config().setHost(host).setPort(port));
    }

    public MockTCPSender(int port)
            throws IOException
    {
        super(new TCPSender.Config().setPort(port));
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        events.addAll(dataList);
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closeCount.incrementAndGet();
    }

    public List<ByteBuffer> getEvents()
    {
        return events;
    }

    public AtomicInteger getCloseCount()
    {
        return closeCount;
    }
}
