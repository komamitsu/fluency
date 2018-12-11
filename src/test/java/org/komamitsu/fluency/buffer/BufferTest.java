/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.buffer;

import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.FluentdRecordFormatter;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class BufferTest
{
    private static final Logger LOG = LoggerFactory.getLogger(BufferTest.class);
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final FluentdRecordFormatter fluentdRecordFormatter = new FluentdRecordFormatter.Config().createInstance();
    private RecordFormatter recordFormatter;
    private Ingester ingester;

    @Before
    public void setUp()
    {
        recordFormatter = mock(RecordFormatter.class);
        ingester = mock(Ingester.class);
    }

    @Test
    public void testBuffer()
            throws IOException
    {
        TestableBuffer buffer = new TestableBuffer.Config().setMaxBufferSize(10000).createInstance(recordFormatter);
        Map<String, Object> data = new HashMap<>();
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

        buffer.flush(ingester, false);
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
        config.createInstance(recordFormatter).clearBackupFiles();

        TestableBuffer buffer = config.createInstance(recordFormatter);
        buffer.close();
        assertEquals(0, buffer.getLoadedBuffers().size());
        buffer.clearBackupFiles();

        List<String> paramOfFirstBuf = Arrays.asList("hello", "42", "world");
        ByteBuffer bufOfFirstBuf = ByteBuffer.wrap("foobar".getBytes(UTF8));
        List<String> paramOfSecondBuf = Arrays.asList("01234567");
        ByteBuffer bufOfSecondBuf = ByteBuffer.wrap(new byte[] {0x00, (byte)0xff});

        buffer = config.createInstance(recordFormatter);
        buffer.setSavableBuffer(paramOfFirstBuf, bufOfFirstBuf);
        buffer.setSavableBuffer(paramOfSecondBuf, bufOfSecondBuf);
        buffer.close();
        assertEquals(0, buffer.getLoadedBuffers().size());

        buffer = config.createInstance(recordFormatter);
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
            config.createInstance(recordFormatter);
            assertTrue(false);
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testFileBackupThatIsWritable()
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
            config.createInstance(recordFormatter);
            assertTrue(false);
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testFileBackupThatIsReadable()
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
            config.createInstance(recordFormatter);
            assertTrue(false);
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void withFluentdFormat()
            throws IOException, InterruptedException
    {
        for (Integer loopCount : Arrays.asList(100, 1000, 10000, 200000)) {
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, false,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, true, false,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, false,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, false, false,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, true, true,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, true, true,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, true, false, true,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
            new BufferTestHelper().baseTestMessageBuffer(loopCount, false, false, true,
                    new Buffer.Config().createInstance(fluentdRecordFormatter));
        }
    }

    @Test
    public void testGetAllocatedSize()
            throws IOException
    {
        Buffer buffer = new Buffer.Config().setChunkInitialSize(256 * 1024)
                .createInstance(fluentdRecordFormatter);
        assertThat(buffer.getAllocatedSize(), is(0L));
        Map<String, Object> map = new HashMap<>();
        map.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foo.bar", new Date().getTime(), map);
        }
        assertThat(buffer.getAllocatedSize(), is(256 * 1024L));
    }

    @Test
    public void testGetBufferedDataSize()
            throws IOException
    {
        Buffer buffer = new Buffer.Config().setChunkInitialSize(256 * 1024)
                .createInstance(fluentdRecordFormatter);
        assertThat(buffer.getBufferedDataSize(), is(0L));

        Map<String, Object> map = new HashMap<>();
        map.put("name", "komamitsu");
        for (int i = 0; i < 10; i++) {
            buffer.append("foo.bar", new Date().getTime(), map);
        }
        assertThat(buffer.getBufferedDataSize(), is(greaterThan(0L)));
        assertThat(buffer.getBufferedDataSize(), is(lessThan(512L)));

        buffer.flush(ingester, true);
        assertThat(buffer.getBufferedDataSize(), is(0L));
    }

    @Test
    public void testAppendIfItDoesNotThrowBufferOverflow()
            throws IOException
    {
        Buffer buffer = new Buffer.Config().setChunkInitialSize(64 * 1024)
                .createInstance(fluentdRecordFormatter);

        StringBuilder buf = new StringBuilder();

        for (int i = 0; i < 1024 * 60; i++) {
            buf.append('x');
        }
        String str60kb = buf.toString();

        for (int i = 0; i < 1024 * 40; i++) {
            buf.append('x');
        }
        String str100kb = buf.toString();

        {
            Map<String, Object> map = new HashMap<>();
            map.put("k", str60kb);
            buffer.append("tag0", new Date().getTime(), map);
        }

        {
            Map<String, Object> map = new HashMap<>();
            map.put("k", str100kb);
            buffer.append("tag0", new Date().getTime(), map);
        }
    }
}