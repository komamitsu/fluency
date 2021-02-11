/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.fluentd.ingester;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RequestOption;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class FluentdIngester
        implements Ingester
{
    private final Config config;
    private final FluentdSender sender;
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    public FluentdIngester(FluentdSender sender)
    {
        this(new Config(), sender);
    }

    public FluentdIngester(Config config, FluentdSender sender)
    {
        this.config = config;
        this.sender = sender;
    }

    @Override
    public void ingest(String tag, ByteBuffer dataBuffer)
            throws IOException
    {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        MessagePacker messagePacker = MessagePack.newDefaultPacker(header);

        int dataLength = dataBuffer.limit();
        messagePacker.packArrayHeader(3);
        messagePacker.packString(tag);
        messagePacker.packRawStringHeader(dataLength);
        messagePacker.flush();

        ByteBuffer headerBuffer = ByteBuffer.wrap(header.toByteArray());

        if (config.isAckResponseMode()) {
            // The spec (https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#entry)
            // says to encode a 128 bit value as base64, but fluent-bit currently does something different
            // and in fluent-bit and fluentd there is no validation. So for now we keep it simple, see
            // discussion on issue #181.
            String token = UUID.randomUUID().toString();

            ByteBuffer optionBuffer = ByteBuffer.wrap(objectMapper.writeValueAsBytes(new RequestOption(dataLength, token)));
            List<ByteBuffer> buffers = Arrays.asList(headerBuffer, dataBuffer, optionBuffer);

            synchronized (sender) {
                sender.sendWithAck(buffers, token);
            }
        }
        else {
            ByteBuffer optionBuffer = ByteBuffer.wrap(objectMapper.writeValueAsBytes(new RequestOption(dataLength, null)));
            List<ByteBuffer> buffers = Arrays.asList(headerBuffer, dataBuffer, optionBuffer);

            synchronized (sender) {
                sender.send(buffers);
            }
        }
    }

    @Override
    public Sender getSender()
    {
        return sender;
    }

    public boolean isAckResponseMode()
    {
        return config.isAckResponseMode();
    }

    @Override
    public void close()
            throws IOException
    {
        sender.close();
    }

    public static class Config
    {
        private boolean ackResponseMode = false;

        public boolean isAckResponseMode()
        {
            return ackResponseMode;
        }

        public void setAckResponseMode(boolean ackResponseMode)
        {
            this.ackResponseMode = ackResponseMode;
        }
    }
}
