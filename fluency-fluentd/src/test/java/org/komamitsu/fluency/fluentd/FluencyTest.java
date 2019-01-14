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

package org.komamitsu.fluency.fluentd;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.komamitsu.fluency.BaseFluencyBuilder;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.recordformat.FluentdRecordFormatter;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.ingester.Ingester;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableRawValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(Theories.class)
public class FluencyTest
{
    @DataPoints
    public static final boolean[] SSL_ENABLED = { false, true };
    private static final Logger LOG = LoggerFactory.getLogger(FluencyTest.class);
    private static final StringValue KEY_OPTION_SIZE = ValueFactory.newString("size");
    private static final StringValue KEY_OPTION_CHUNK = ValueFactory.newString("chunk");
    private Ingester ingester;

    @Before
    public void setUp()
    {
        ingester = mock(Ingester.class);
    }

    @Test
    public void testSenderErrorHandler()
            throws IOException, InterruptedException
    {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> errorContainer = new AtomicReference<Throwable>();

        Fluency fluency = new FluencyBuilder().build(Integer.MAX_VALUE,
                new FluencyBuilder.FluencyConfig()
                        .setSenderMaxRetryCount(1)
                        .setErrorHandler(e -> {
                            errorContainer.set(e);
                            countDownLatch.countDown();
                        }));

        HashMap<String, Object> event = new HashMap<>();
        event.put("name", "foo");
        fluency.emit("tag", event);

        if (!countDownLatch.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Timeout");
        }

        assertThat(errorContainer.get(), is(instanceOf(RetryableSender.RetryOverException.class)));
    }

    @Theory
    public void testWithoutAckResponse(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                clientSocket -> {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                    assertEquals(3, unpacker.unpackArrayHeader());
                    assertEquals("foo.bar", unpacker.unpackString());
                    ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                    Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                    assertEquals(1, map.size());
                    assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                    unpacker.close();
                },
                serverPort -> {
                    Fluency fluency =
                            new FluencyBuilder().build(serverPort,
                                    new FluencyBuilder.FluencyConfig().setSslEnabled(sslEnabled));
                    fluency.emit("foo.bar", new HashMap<>());
                    fluency.close();
                }, 5000);
        assertNull(exception);
    }

    @Theory
    public void testWithAckResponseButNotReceiveToken(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                clientSocket -> {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                    assertEquals(3, unpacker.unpackArrayHeader());
                    assertEquals("foo.bar", unpacker.unpackString());
                    ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                    Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                    assertEquals(2, map.size());
                    assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                    assertNotNull(map.get(KEY_OPTION_CHUNK).asRawValue().asString());
                    unpacker.close();
                },
                serverPort -> {
                    Fluency fluency =
                            new FluencyBuilder().build(serverPort,
                                    new FluencyBuilder.FluencyConfig().setSslEnabled(sslEnabled).setAckResponseMode(true));
                    fluency.emit("foo.bar", new HashMap<>());
                    fluency.close();
                }, 5000);
        assertEquals(exception.getClass(), TimeoutException.class);
    }

    @Theory
    public void testWithAckResponseButWrongReceiveToken(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                clientSocket -> {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                    assertEquals(3, unpacker.unpackArrayHeader());
                    assertEquals("foo.bar", unpacker.unpackString());
                    ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                    Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                    assertEquals(2, map.size());
                    assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                    assertNotNull(map.get(KEY_OPTION_CHUNK).asRawValue().asString());

                    MessagePacker packer = MessagePack.newDefaultPacker(clientSocket.getOutputStream());
                    packer.packMapHeader(1)
                            .packString("ack").packString(UUID.randomUUID().toString())
                            .close();

                    // Close the input stream after closing the output stream to avoid closing a socket too early
                    unpacker.close();
                },
                serverPort -> {
                    Fluency fluency =
                            new FluencyBuilder().build(serverPort,
                                    new FluencyBuilder.FluencyConfig().setSslEnabled(sslEnabled).setAckResponseMode(true));
                    fluency.emit("foo.bar", new HashMap<>());
                    fluency.close();
                }, 5000);
        assertEquals(exception.getClass(), TimeoutException.class);
    }

    @Theory
    public void testWithAckResponseWithProperToken(final boolean sslEnabled)
            throws Throwable
    {
        Exception exception = new ConfigurableTestServer(sslEnabled).run(
                clientSocket -> {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(clientSocket.getInputStream());
                    assertEquals(3, unpacker.unpackArrayHeader());
                    assertEquals("foo.bar", unpacker.unpackString());
                    ImmutableRawValue rawValue = unpacker.unpackValue().asRawValue();
                    Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
                    assertEquals(2, map.size());
                    assertEquals(rawValue.asByteArray().length, map.get(KEY_OPTION_SIZE).asIntegerValue().asInt());
                    String ackResponseToken = map.get(KEY_OPTION_CHUNK).asRawValue().asString();
                    assertNotNull(ackResponseToken);

                    MessagePacker packer = MessagePack.newDefaultPacker(clientSocket.getOutputStream());
                    packer.packMapHeader(1)
                            .packString("ack").packString(ackResponseToken)
                            .close();

                    // Close the input stream after closing the output stream to avoid closing a socket too early
                    unpacker.close();
                },
                serverPort -> {
                    Fluency fluency = new FluencyBuilder().build(serverPort,
                            new FluencyBuilder.FluencyConfig().setSslEnabled(sslEnabled).setAckResponseMode(true));
                    fluency.emit("foo.bar", new HashMap<>());
                    fluency.close();
                }, 5000);
        assertNull(exception);
    }

    static class Foo
    {
        String s;
    }

    public static class FooSerializer
            extends StdSerializer<Foo>
    {
        final AtomicBoolean serialized;

        FooSerializer(AtomicBoolean serialized)
        {
            super(Foo.class);
            this.serialized = serialized;
        }

        @Override
        public void serialize(Foo value, JsonGenerator gen, SerializerProvider provider)
                throws IOException
        {
            gen.writeStartObject();
            gen.writeStringField("s", "Foo:" + value.s);
            gen.writeEndObject();
            serialized.set(true);
        }
    }

    @Test
    public void testBufferWithJacksonModule()
            throws IOException
    {
        AtomicBoolean serialized = new AtomicBoolean();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(Foo.class, new FooSerializer(serialized));

        Buffer.Config bufferConfig = new Buffer
                .Config()
                .setChunkInitialSize(64)
                .setMaxBufferSize(256);

        FluentdRecordFormatter.Config fluentdRecordFormatterWithModuleConfig =
                new FluentdRecordFormatter.Config().setJacksonModules(Collections.singletonList(simpleModule));

        Fluency fluency = new BaseFluencyBuilder().buildFromConfigs(
                    fluentdRecordFormatterWithModuleConfig,
                    bufferConfig,
                    new AsyncFlusher.Config(),
                    ingester);

        Map<String, Object> event = new HashMap<>();
        Foo foo = new Foo();
        foo.s = "Hello";
        event.put("foo", foo);
        fluency.emit("tag", event);

        assertThat(serialized.get(), is(true));
    }
}
