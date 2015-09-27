package org.komamitsu.fluency.buffer;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class MessageBufferTest
{
    @Test
    public void testMessageBuffer()
            throws IOException, InterruptedException
    {
        for (Integer loopCount : Arrays.asList(100, 1000, 10000)) {
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, true, true, new MessageBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, false, true, new MessageBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, true, false, new MessageBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, false, false, new MessageBuffer.Config().createInstance());
        }
    }
}