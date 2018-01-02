package org.komamitsu.fluency.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class MockTCPSender
        extends TCPSender
{
    private static final Logger LOG = LoggerFactory.getLogger(MockTCPSender.class);

    private final List<Event> events = new ArrayList<Event>();
    private final AtomicInteger closeCount = new AtomicInteger();

    public class Event
    {
        ByteBuffer header;
        ByteBuffer data;
        ByteBuffer option;

        public byte[] getAllBytes()
        {
            byte[] bytes = new byte[header.limit() + data.limit() + option.limit()];

            int pos = 0;
            int len = header.limit();
            header.get(bytes, pos, len);
            pos += len;

            len = data.limit();
            data.get(bytes, pos, len);
            pos += len;

            len = option.limit();
            option.get(bytes, pos, len);
            pos += len;

            return bytes;
        }
    }

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
        assertEquals(3, dataList.size());
        Event event = new Event();
        int i = 0;
        for (ByteBuffer data : dataList) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(data.capacity());
            byteBuffer.put(data);
            byteBuffer.flip();
            switch (i) {
                case 0:
                    event.header = byteBuffer;
                    break;
                case 1:
                    event.data = byteBuffer;
                    break;
                case 2:
                    event.option = byteBuffer;
                    break;
                default:
                    throw new IllegalStateException("Shouldn't reach here");
            }
            i++;
        }
        events.add(event);
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closeCount.incrementAndGet();
    }

    public List<Event> getEvents()
    {
        return events;
    }

    public AtomicInteger getCloseCount()
    {
        return closeCount;
    }
}
