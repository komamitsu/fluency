package org.komamitsu.fluency.sender;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.*;

public class MessagePackAckTokenSerDeTest
{
    @Test
    public void testPackUnpack()
            throws IOException
    {
        Charset charset = Charset.forName("ASCII");
        MessagePackAckTokenSerDe serDe = new MessagePackAckTokenSerDe();
        for (int i = 0; i < 1000; i++) {
            byte[] token = UUID.randomUUID().toString().getBytes(charset);
            byte[] packedToken = serDe.pack(token);
            byte[] unpackedToken = serDe.unpack(packedToken);
            assertTrue(Arrays.equals(token, unpackedToken));
        }
    }
}