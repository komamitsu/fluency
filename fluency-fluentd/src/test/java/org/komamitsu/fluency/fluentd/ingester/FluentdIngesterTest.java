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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.komamitsu.fluency.fluentd.ingester.FluentdIngester;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.ingester.Ingester;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class FluentdIngesterTest
{
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final String TAG = "foo.bar";
    private static final byte[] DATA = "hello, world".getBytes(CHARSET);
    private FluentdSender fluentdSender;

    @Captor
    public ArgumentCaptor<List<ByteBuffer>> byteBuffersArgumentCaptor;

    @Before
    public void setUp()
            throws Exception
    {
        fluentdSender = mock(FluentdSender.class);
    }

    private byte[] getIngestedData(List<ByteBuffer> byteBuffers)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (ByteBuffer byteBuffer : byteBuffers) {
            outputStream.write(byteBuffer.array());
        }
        return outputStream.toByteArray();
    }

    @Test
    public void ingestWithoutAck()
            throws IOException
    {
        Ingester ingester = new FluentdIngester(new FluentdIngester.Config(), fluentdSender);
        ingester.ingest(TAG, ByteBuffer.wrap(DATA));

        verify(fluentdSender, times(1)).send(byteBuffersArgumentCaptor.capture());
        List<ByteBuffer> byteBuffers = byteBuffersArgumentCaptor.getAllValues().get(0);
        byte[] ingested = getIngestedData(byteBuffers);

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(ingested);
        ImmutableArrayValue arrayValue = unpacker.unpackValue().asArrayValue();
        assertEquals(3, arrayValue.size());
        assertEquals(TAG, arrayValue.get(0).asStringValue().asString());
        assertArrayEquals(DATA, arrayValue.get(1).asRawValue().asByteArray());
        Map<Value, Value> options = arrayValue.get(2).asMapValue().map();
        assertEquals(1, options.size());
        assertEquals(DATA.length,
                options.get(ValueFactory.newString("size")).asIntegerValue().asInt());
    }

    @Test
    public void ingestWithAck()
            throws IOException
    {
        FluentdIngester.Config config = new FluentdIngester.Config();
        config.setAckResponseMode(true);
        Ingester ingester = new FluentdIngester(config, fluentdSender);
        ingester.ingest(TAG, ByteBuffer.wrap(DATA));

        ArgumentCaptor<byte[]> ackTokenArgumentCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(fluentdSender, times(1))
                .sendWithAck(byteBuffersArgumentCaptor.capture(), ackTokenArgumentCaptor.capture());
        List<ByteBuffer> byteBuffers = byteBuffersArgumentCaptor.getAllValues().get(0);
        byte[] ingested = getIngestedData(byteBuffers);

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(ingested);
        ImmutableArrayValue arrayValue = unpacker.unpackValue().asArrayValue();
        assertEquals(3, arrayValue.size());
        assertEquals(TAG, arrayValue.get(0).asStringValue().asString());
        assertArrayEquals(DATA, arrayValue.get(1).asRawValue().asByteArray());
        Map<Value, Value> options = arrayValue.get(2).asMapValue().map();
        assertEquals(2, options.size());
        assertEquals(DATA.length, options.get(ValueFactory.newString("size")).asIntegerValue().asInt());
        String ackToken = options.get(ValueFactory.newString("chunk")).asRawValue().asString();
        UUID uuidFromAckToken = UUID.fromString(ackToken);

        List<byte[]> ackTokenArgumentCaptorAllValues = ackTokenArgumentCaptor.getAllValues();
        assertEquals(1, ackTokenArgumentCaptorAllValues.size());
        assertEquals(uuidFromAckToken,
                UUID.fromString(new String(ackTokenArgumentCaptorAllValues.get(0), CHARSET)));
    }

    @Test
    public void getSender()
    {
        assertEquals(fluentdSender, new FluentdIngester(new FluentdIngester.Config(), fluentdSender).getSender());
    }

    @Test
    public void close()
            throws IOException
    {
        Ingester ingester = new FluentdIngester(new FluentdIngester.Config(), fluentdSender);
        ingester.close();

        verify(fluentdSender, times(1)).close();
    }
}