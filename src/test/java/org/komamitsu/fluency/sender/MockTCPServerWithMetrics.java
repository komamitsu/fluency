package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.MockTCPServer;
import org.komamitsu.fluency.util.Tuple;

import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MockTCPServerWithMetrics
        extends MockTCPServer
{
    private final List<Tuple<Type, Integer>> events = new CopyOnWriteArrayList<Tuple<Type, Integer>>();
    private final EventHandler eventHandler = new EventHandler() {
        @Override
        public void onConnect(Socket acceptSocket)
        {
            events.add(new Tuple<Type, Integer>(Type.CONNECT, null));
        }

        @Override
        public void onReceive(Socket acceptSocket, int len, byte[] data)
        {
            events.add(new Tuple<Type, Integer>(Type.RECEIVE, len));
        }

        @Override
        public void onClose(Socket acceptSocket)
        {
            events.add(new Tuple<Type, Integer>(Type.CLOSE, null));
        }
    };

    public MockTCPServerWithMetrics(boolean sslEnabled)
    {
        super(sslEnabled);
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
