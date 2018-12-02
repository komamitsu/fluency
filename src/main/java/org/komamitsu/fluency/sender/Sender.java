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

package org.komamitsu.fluency.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class Sender
    implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);
    private final Config config;

    protected Sender(Config config)
    {
        this.config = config;
    }

    public synchronized void send(ByteBuffer buffer)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(Arrays.asList(buffer), null);
    }

    public synchronized void send(List<ByteBuffer> buffers)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(buffers, null);
    }

    public void sendWithAck(List<ByteBuffer> buffers, byte[] ackToken)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(buffers, ackToken);
    }

    private void sendInternalWithRestoreBufferPositions(List<ByteBuffer> buffers, byte[] ackToken)
            throws IOException
    {
        List<Integer> positions = new ArrayList<Integer>(buffers.size());
        for (ByteBuffer data : buffers) {
            positions.add(data.position());
        }

        try {
            sendInternal(buffers, ackToken);
        }
        catch (Exception e) {
            for (int i = 0; i < buffers.size(); i++) {
                buffers.get(i).position(positions.get(i));
            }

            if (config.senderErrorHandler != null) {
                try {
                    config.senderErrorHandler.handle(e);
                }
                catch (Exception ex) {
                    LOG.warn("Failed to handle an error in the error handler {}", config.senderErrorHandler, ex);
                }
            }

            if (e instanceof IOException) {
                throw (IOException)e;
            }
            else {
                throw new IOException(e);
            }
        }
    }

    public abstract boolean isAvailable();

    abstract protected void sendInternal(List<ByteBuffer> buffers, byte[] ackToken) throws IOException;

    public static class Config
    {
        private SenderErrorHandler senderErrorHandler;

        public SenderErrorHandler getSenderErrorHandler()
        {
            return senderErrorHandler;
        }

        public Config setSenderErrorHandler(SenderErrorHandler senderErrorHandler)
        {
            this.senderErrorHandler = senderErrorHandler;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "senderErrorHandler=" + senderErrorHandler +
                    '}';
        }
    }

    public interface Instantiator
    {
        Sender createInstance();
    }
}
