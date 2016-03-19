package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.StubSender;
import org.komamitsu.fluency.util.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class BufferTest
{
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test
    public void testBuffer()
            throws IOException
    {
        TestableBuffer buffer = new TestableBuffer.Config().setMaxBufferSize(10000).createInstance();
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(1000, buffer.getAllocatedSize());
        assertEquals(0.1, buffer.getBufferUsage(), 0.001);
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(2000, buffer.getAllocatedSize());
        assertEquals(0.2, buffer.getBufferUsage(), 0.001);

        buffer.flush(new StubSender(), false);
        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferUsage(), 0.001);
    }

    @Test
    public void testFileBackup()
    {
        TestableBuffer.Config config = new TestableBuffer.Config().
                setFileBackupDir(System.getProperty("java.io.tmpdir")).
                setFileBackupPrefix("FileBackupTest");

        // Just for cleaning backup files
        config.createInstance().clearBackupFiles();

        TestableBuffer buffer = config.createInstance();
        buffer.close();
        assertEquals(0, buffer.getLoadedBuffers().size());
        buffer.clearBackupFiles();

        List<String> paramOfFirstBuf = Arrays.asList("hello", "42", "world");
        ByteBuffer bufOfFirstBuf = ByteBuffer.wrap("foobar".getBytes(UTF8));
        List<String> paramOfSecondBuf = Arrays.asList("01234567");
        ByteBuffer bufOfSecondBuf = ByteBuffer.wrap(new byte[] {0x00, (byte)0xff});

        buffer = config.createInstance();
        buffer.setSavableBuffer(paramOfFirstBuf, bufOfFirstBuf);
        buffer.setSavableBuffer(paramOfSecondBuf, bufOfSecondBuf);
        buffer.close();
        assertEquals(0, buffer.getLoadedBuffers().size());

        buffer = config.createInstance();
        buffer.close();
        assertEquals(2, buffer.getLoadedBuffers().size());

        bufOfFirstBuf.flip();
        bufOfSecondBuf.flip();
        for (Tuple<List<String>, ByteBuffer> loadedBuffer : buffer.getLoadedBuffers()) {
            ByteBuffer expected = null;
            ByteBuffer actual = null;
            if (loadedBuffer.getFirst().equals(paramOfFirstBuf)) {
                expected = loadedBuffer.getSecond();
                actual = bufOfFirstBuf;
            }
            else if (loadedBuffer.getFirst().equals(paramOfSecondBuf)) {
                expected = loadedBuffer.getSecond();
                actual = bufOfSecondBuf;
            }
            else {
                assertTrue(false);
            }

            assertEquals(expected.remaining(), actual.remaining());
            for (int i = 0; i < expected.remaining(); i++) {
                assertEquals(expected.get(i), actual.get(i));
            }
        }
    }
}