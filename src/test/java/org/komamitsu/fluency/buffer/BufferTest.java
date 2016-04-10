package org.komamitsu.fluency.buffer;

import org.junit.Test;
import org.komamitsu.fluency.StubSender;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class BufferTest
{
    private static final Logger LOG = LoggerFactory.getLogger(BufferTest.class);
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
        assertEquals(TestableBuffer.ALLOC_SIZE * 10, buffer.getAllocatedSize());
        assertEquals(TestableBuffer.RECORD_DATA_SIZE * 10, buffer.getBufferedDataSize());
        assertEquals(TestableBuffer.ALLOC_SIZE * 10 / 10000f, buffer.getBufferUsage(), 0.001);
        for (int i = 0; i < 10; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(TestableBuffer.ALLOC_SIZE * 20, buffer.getAllocatedSize());
        assertEquals(TestableBuffer.RECORD_DATA_SIZE * 20, buffer.getBufferedDataSize());
        assertEquals(TestableBuffer.ALLOC_SIZE * 20 / 10000f, buffer.getBufferUsage(), 0.001);

        buffer.flush(new StubSender(), false);
        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferedDataSize());
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

    @Test
    public void testFileBackupThatIsNotDirectory()
            throws IOException
    {
        File backupDirFile = File.createTempFile("testFileBackupWithInvalidDir", ".tmp");
        backupDirFile.deleteOnExit();
        TestableBuffer.Config config = new TestableBuffer.Config().setFileBackupDir(backupDirFile.getAbsolutePath());

        try {
            config.createInstance();
            assertTrue(false);
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testFileBackupThatIsWritable()
            throws IOException
    {
        File backupDir = new File(System.getProperties().getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        if (backupDir.mkdir()) {
            LOG.info("Created directory: {}", backupDir);
        }
        if (!backupDir.setWritable(false)) {
            throw new RuntimeException("Failed to revoke writable permission");
        }
        backupDir.deleteOnExit();
        TestableBuffer.Config config = new TestableBuffer.Config().setFileBackupDir(backupDir.getAbsolutePath());

        try {
            config.createInstance();
            assertTrue(false);
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testFileBackupThatIsReadable()
            throws IOException
    {
        File backupDir = new File(System.getProperties().getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        if (backupDir.mkdir()) {
            LOG.info("Created directory: {}", backupDir);
        }
        if (!backupDir.setReadable(false)) {
            throw new RuntimeException("Failed to revoke readable permission");
        }
        backupDir.deleteOnExit();
        TestableBuffer.Config config = new TestableBuffer.Config().setFileBackupDir(backupDir.getAbsolutePath());

        try {
            config.createInstance();
            assertTrue(false);
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }
}