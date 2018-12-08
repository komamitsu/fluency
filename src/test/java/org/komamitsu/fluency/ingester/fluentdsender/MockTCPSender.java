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

package org.komamitsu.fluency.ingester.fluentdsender;

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

    public static class Event
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
    protected synchronized void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
            throws IOException
    {
        assertEquals(3, buffers.size());
        Event event = new Event();
        int i = 0;
        for (ByteBuffer data : buffers) {
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
