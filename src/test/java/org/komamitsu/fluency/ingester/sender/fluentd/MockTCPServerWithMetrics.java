/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.ingester.sender.fluentd;

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
