package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.util.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MockTCPServerWithMetrics extends AbstractMockTCPServer
{
    private final List<Tuple<Type, Integer>> events = new CopyOnWriteArrayList<Tuple<Type, Integer>>();
    private final EventHandler eventHandler = new EventHandler() {
        @Override
        public void onConnect(SocketChannel acceptSocketChannel)
        {
            events.add(new Tuple<Type, Integer>(Type.CONNECT, null));
        }

        @Override
        public void onReceive(SocketChannel acceptSocketChannel, ByteBuffer data)
        {
            events.add(new Tuple<Type, Integer>(Type.RECEIVE, data.position()));
        }

        @Override
        public void onClose(SocketChannel acceptSocketChannel)
        {
            events.add(new Tuple<Type, Integer>(Type.CLOSE, null));
        }
    };

    public MockTCPServerWithMetrics()
            throws IOException
    {
    }

    @Override
    protected EventHandler getEventHandler()
    {
        return eventHandler;
    }

    public enum Type
    {
        CONNECT, RECEIVE, CLOSE;
    }

    public List<Tuple<Type, Integer>> getEvents()
    {
        return events;
    }

}
