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

package org.komamitsu.fluency.ingester;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.komamitsu.fluency.ingester.sender.fluentd.FluentdSender;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.any;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FluentdIngesterTest
{
    private static final String TAG = "foo.bar";
    private static final byte[] DATA = "hello, world".getBytes(Charset.forName("UTF-8"));

    @Before
    public void setUp()
            throws Exception
    {
    }

    @Captor
    public ArgumentCaptor<List<ByteBuffer>> byteBuffersArgumentCaptor;

    @Test
    public void ingest()
            throws IOException
    {
        FluentdSender fluentdSender = mock(FluentdSender.class);
        Ingester ingester = new FluentdIngester.Config().createInstance(fluentdSender);
        ingester.ingest(TAG, ByteBuffer.wrap(DATA));

        verify(fluentdSender, times(1)).send(byteBuffersArgumentCaptor.capture());
        List<ByteBuffer> byteBuffers = byteBuffersArgumentCaptor.getAllValues().get(0);
        byte[] ingested;
        {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (ByteBuffer byteBuffer : byteBuffers) {
                outputStream.write(byteBuffer.array());
            }
            ingested = outputStream.toByteArray();
        }
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(ingested);
        ImmutableArrayValue arrayValue = unpacker.unpackValue().asArrayValue();
        assertEquals(3, arrayValue.size());
        assertEquals(TAG, arrayValue.get(0).asStringValue().asString());
        assertArrayEquals(DATA, arrayValue.get(1).asRawValue().asByteArray());
        Map<Value, Value> options = arrayValue.get(2).asMapValue().map();
        assertEquals(1, options.size());
        assertEquals(DATA.length, options.get(ValueFactory.newString("size")).asIntegerValue().asInt());
    }

    @Test
    public void getSender()
    {
    }

    @Test
    public void isAckResponseMode()
    {
    }

    @Test
    public void close()
    {
    }
}