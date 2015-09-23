package org.komamitsu.fluency.buffer;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class PackedForwardBufferTest
{
    @Test
    public void testPackedForwardBuffer()
            throws IOException, InterruptedException
    {
        for (Integer loopCount : Arrays.asList(100, 1000, 10000)) {
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, true, new PackedForwardBuffer(new PackedForwardBuffer.Config()));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, true, new PackedForwardBuffer(new PackedForwardBuffer.Config()));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, false, new PackedForwardBuffer(new PackedForwardBuffer.Config()));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, false, new PackedForwardBuffer(new PackedForwardBuffer.Config()));
        }
    }
}