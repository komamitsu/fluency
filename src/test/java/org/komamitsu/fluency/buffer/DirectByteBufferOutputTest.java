package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.MessageBuffer;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class DirectByteBufferOutputTest
{
    @Test
    public void test()
            throws IOException
    {
        DirectByteBufferOutput bufferOutput = new DirectByteBufferOutput();
        MessagePack.ConfigBuilder configBuilder = new MessagePack.ConfigBuilder();
        MessagePack.Config config = configBuilder.packerBufferSize(32).build();
        MessagePacker packer = new MessagePacker(bufferOutput, config);
        packer.packInt(42);
        byte[] buf = new byte[70];
        for (int i = 0; i < buf.length; i++) {
            buf[i] = (byte) 0xDD;
        }
        packer.packBinaryHeader(buf.length);
        packer.writePayload(buf);
        packer.close();

        int totalReadLen = 1 + 2 + 70;
        assertEquals(totalReadLen, bufferOutput.getTotalWrittenBytes());

        List<MessageBuffer> messageBuffers = bufferOutput.getMessageBuffers();
        assertEquals(3, messageBuffers.size());

        MessageBuffer messageBuffer = messageBuffers.remove(0);
        assertEquals(0, messageBuffer.getReference().position());
        assertEquals(32, messageBuffer.getReference().limit());
        int idx = 0;
        assertEquals((byte)42, messageBuffer.getReference().get(idx++));
        assertEquals((byte)0xC4, messageBuffer.getReference().get(idx++));
        assertEquals((byte)70, messageBuffer.getReference().get(idx++));
        for (; idx < 32; idx++) {
            assertEquals((byte)0xDD, messageBuffer.getReference().get(idx++));
        }

        messageBuffer = messageBuffers.remove(0);
        assertEquals(0, messageBuffer.getReference().position());
        assertEquals(32, messageBuffer.getReference().limit());
        for (idx = 0; idx < 32; idx++) {
            assertEquals((byte)0xDD, messageBuffer.getReference().get(idx++));
        }

        messageBuffer = messageBuffers.remove(0);
        int limit = totalReadLen - 32 - 32;
        assertEquals(0, messageBuffer.getReference().position());
        assertEquals(limit, messageBuffer.getReference().limit());
        for (idx = 0; idx < limit; idx++) {
            assertEquals((byte) 0xDD, messageBuffer.getReference().get(idx++));
        }
    }
}