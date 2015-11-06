package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.*;
import org.msgpack.core.buffer.MessageBuffer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class DirectByteBufferInputTest
{
    @Test
    public void test()
            throws IOException
    {
        List<MessageBuffer> messageBuffers = new LinkedList<MessageBuffer>();

        MessageBuffer messageBuffer = MessageBuffer.newDirectBuffer(32);
        int idx = 0;
        messageBuffer.putByte(idx++, (byte) 42);    // int:42
        messageBuffer.putByte(idx++, (byte) 0xC4);  // bin8
        messageBuffer.putByte(idx++, (byte) 70);    //   len:70
        for (; idx < 32; idx++) {
            messageBuffer.putByte(idx, (byte) 0xDD);
        }
        messageBuffers.add(messageBuffer);

        messageBuffer = MessageBuffer.newDirectBuffer(32);
        idx = 0;
        for (; idx < 32; idx++) {
            messageBuffer.putByte(idx, (byte) 0xDD);
        }
        messageBuffers.add(messageBuffer);

        messageBuffer = MessageBuffer.newDirectBuffer(32);
        idx = 0;
        int len = (1 + 2 + 70) - 32 - 32;
        for (; idx < len; idx++) {
            messageBuffer.putByte(idx, (byte) 0xDD);
        }
        messageBuffers.add(messageBuffer);

        DirectByteBufferInput bufferInput = new DirectByteBufferInput(messageBuffers);
        MessageUnpacker unpacker = new MessageUnpacker(bufferInput);
        assertEquals(42, unpacker.unpackInt());
        assertEquals(70, unpacker.unpackRawStringHeader());
        byte[] bytes = unpacker.readPayload(70);
        for (byte b : bytes) {
            assertEquals((byte)0xDD, b);
        }
    }
}