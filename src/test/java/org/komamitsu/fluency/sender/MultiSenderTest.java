package org.komamitsu.fluency.sender;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class MultiSenderTest
{
    @Test
    public void testConstructor()
            throws IOException
    {
        MultiSender multiSender = new MultiSender(Arrays.asList(new TCPSender(24225), new TCPSender("0.0.0.0", 24226)));
        assertEquals(2, multiSender.sendersAndFailureDetectors.size());

        assertEquals("127.0.0.1", multiSender.sendersAndFailureDetectors.get(0).getFirst().getHost());
        assertEquals(24225, multiSender.sendersAndFailureDetectors.get(0).getFirst().getPort());
        assertEquals("127.0.0.1", multiSender.sendersAndFailureDetectors.get(0).getSecond().getHeartbeater().getHost());
        assertEquals(24225, multiSender.sendersAndFailureDetectors.get(0).getSecond().getHeartbeater().getPort());

        assertEquals("0.0.0.0", multiSender.sendersAndFailureDetectors.get(1).getFirst().getHost());
        assertEquals(24226, multiSender.sendersAndFailureDetectors.get(1).getFirst().getPort());
        assertEquals("0.0.0.0", multiSender.sendersAndFailureDetectors.get(1).getSecond().getHeartbeater().getHost());
        assertEquals(24226, multiSender.sendersAndFailureDetectors.get(1).getSecond().getHeartbeater().getPort());
    }
}