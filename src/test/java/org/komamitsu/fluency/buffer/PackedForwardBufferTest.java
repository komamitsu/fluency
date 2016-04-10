package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.sender.MockTCPSender;
import org.komamitsu.fluency.sender.Sender;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class PackedForwardBufferTest
{
    @Test
    public void testPackedForwardBuffer()
            throws IOException, InterruptedException
    {
        for (Integer loopCount : Arrays.asList(100, 1000, 10000)) {
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, true, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, true, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, false, new PackedForwardBuffer.Config().createInstance());
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, false, new PackedForwardBuffer.Config().createInstance());
        }
    }

    @Test
    public void testGetAllocatedSize()
            throws IOException
    {
        PackedForwardBuffer buffer = new PackedForwardBuffer.Config().setInitialBufferSize(256 * 1024).createInstance();
        assertThat(buffer.getAllocatedSize(), is(0L));
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foo.bar", new Date().getTime(), map);
        }
        assertThat(buffer.getAllocatedSize(), is(256 * 1024L));
    }

    @Test
    public void testGetBufferedDataSize()
            throws IOException, InterruptedException
    {
        PackedForwardBuffer buffer = new PackedForwardBuffer.Config().setInitialBufferSize(256 * 1024).createInstance();
        assertThat(buffer.getBufferedDataSize(), is(0L));

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foo.bar", new Date().getTime(), map);
        }
        assertThat(buffer.getBufferedDataSize(), is(greaterThan(0L)));
        assertThat(buffer.getBufferedDataSize(), is(lessThan(512L)));

        MockTCPSender sender = new MockTCPSender(24224);
        buffer.flush(sender, true);
        assertThat(buffer.getBufferedDataSize(), is(0L));
    }
}