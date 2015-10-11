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
    public void testPack()
            throws IOException
    {
        MessagePackAckTokenSerDe serDe = new MessagePackAckTokenSerDe();
        byte[] token = UUID.randomUUID().toString().getBytes(CHARSET);
        byte[] packedToken = serDe.pack(token);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packedToken);
        ImmutableValue value = unpacker.unpackValue();
        assertEquals(ValueType.MAP, value.getValueType());
        ImmutableMapValue mapValue = value.asMapValue();
        assertEquals(1, mapValue.size());
        assertArrayEquals(token, mapValue.map().get(ValueFactory.newString("chunk")).asBinaryValue().asByteArray());
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