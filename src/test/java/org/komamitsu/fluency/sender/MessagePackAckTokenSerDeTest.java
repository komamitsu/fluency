package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.ValueType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import static org.junit.Assert.*;

public class MessagePackAckTokenSerDeTest
{
    private static final Charset CHARSET = Charset.forName("ASCII");

    @Test
    public void testPackWithAckResponseToken()
            throws IOException
    {
        MessagePackAckTokenSerDe serDe = new MessagePackAckTokenSerDe();
        byte[] ackResponseToken = UUID.randomUUID().toString().getBytes(CHARSET);
        int size = Integer.MAX_VALUE;
        byte[] packedToken = serDe.packWithAckResponseToken(size, ackResponseToken);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packedToken);
        ImmutableValue value = unpacker.unpackValue();
        assertEquals(ValueType.MAP, value.getValueType());
        ImmutableMapValue mapValue = value.asMapValue();
        assertEquals(2, mapValue.size());
        assertArrayEquals(ackResponseToken, mapValue.map().get(ValueFactory.newString("chunk")).asBinaryValue().asByteArray());
        assertEquals(size, mapValue.map().get(ValueFactory.newString("size")).asIntegerValue().asInt());
    }

    @Test
    public void testPackWithoutAckResponseToken()
            throws IOException
    {
        MessagePackAckTokenSerDe serDe = new MessagePackAckTokenSerDe();
        int size = Integer.MAX_VALUE;
        byte[] packedToken = serDe.packWithAckResponseToken(size, null);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packedToken);
        ImmutableValue value = unpacker.unpackValue();
        assertEquals(ValueType.MAP, value.getValueType());
        ImmutableMapValue mapValue = value.asMapValue();
        assertEquals(1, mapValue.size());
        assertEquals(size, mapValue.map().get(ValueFactory.newString("size")).asIntegerValue().asInt());
    }

    @Test
    public void testUnpack()
            throws IOException
    {
        MessagePackAckTokenSerDe serDe = new MessagePackAckTokenSerDe();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MessagePacker packer = MessagePack.newDefaultPacker(outputStream);
        byte[] token = UUID.randomUUID().toString().getBytes(CHARSET);
        packer.packMapHeader(1).
                packString("ack").
                packBinaryHeader(token.length).writePayload(token).
                close();
        byte[] unpackedToken = serDe.unpack(outputStream.toByteArray());
        assertArrayEquals(token, unpackedToken);
    }
}