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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.JsonRecordFormatter;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.komamitsu.fluency.util.Tuple;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BufferTest
{
    private static final Logger LOG = LoggerFactory.getLogger(BufferTest.class);
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private Ingester ingester;
    private RecordFormatter recordFormatter;
    private Buffer.Config bufferConfig;

    @Before
    public void setUp()
    {
        recordFormatter = new JsonRecordFormatter();
        ingester = mock(Ingester.class);
        bufferConfig = new Buffer.Config();
    }

    @Test
    public void testBuffer()
            throws IOException
    {
        int recordSize = 20; // '{"name":"komamitsu"}'
        int tags = 10;
        int allocSizePerBuf = recordSize;
        float allocRatio = 2f;
        int maxAllocSize = (allocSizePerBuf * 4) * tags;

        bufferConfig.setChunkInitialSize(allocSizePerBuf);
        bufferConfig.setChunkExpandRatio(allocRatio);
        bufferConfig.setMaxBufferSize(maxAllocSize);
        Buffer buffer = new Buffer(bufferConfig, recordFormatter);

        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferedDataSize());
        assertEquals(0f, buffer.getBufferUsage(), 0.001);

        Map<String, Object> data = new HashMap<>();
        data.put("name", "komamitsu");
        for (int i = 0; i < tags; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(allocSizePerBuf * tags, buffer.getAllocatedSize());
        assertEquals(recordSize * tags, buffer.getBufferedDataSize());
        assertEquals(((float) allocSizePerBuf) * tags / maxAllocSize, buffer.getBufferUsage(), 0.001);
        for (int i = 0; i < tags; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        float totalAllocatedSize = allocSizePerBuf * (1 + allocRatio) * tags;
        assertEquals(totalAllocatedSize, buffer.getAllocatedSize(), 0.001);
        assertEquals(2L * recordSize * tags, buffer.getBufferedDataSize());
        assertEquals(totalAllocatedSize / maxAllocSize, buffer.getBufferUsage(), 0.001);

        buffer.flush(ingester, true);
        assertEquals(totalAllocatedSize, buffer.getAllocatedSize(), 0.001);
        assertEquals(0, buffer.getBufferedDataSize());
        assertEquals(totalAllocatedSize / maxAllocSize, buffer.getBufferUsage(), 0.001);

        buffer.close();
        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferedDataSize());
        assertEquals(0, buffer.getBufferUsage(), 0.001);
    }

    @Test
    public void testFileBackup()
            throws IOException
    {
        bufferConfig.setFileBackupDir(System.getProperty("java.io.tmpdir"));
        bufferConfig.setFileBackupPrefix("FileBackupTest");

        // Just for cleaning backup files
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {
            buffer.clearBackupFiles();
        }
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {
            assertEquals(0, buffer.getBufferedDataSize());
        }

        long currentTime = System.currentTimeMillis() / 1000;
        Map<String, Object> event0 = ImmutableMap.of("name", "a", "age", 42);
        Map<String, Object> event1 = ImmutableMap.of("name", "b", "age", 99);
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {
            buffer.append("foo", currentTime, event0);
            buffer.append("bar", currentTime, event1);
        }

        Ingester ingester = mock(Ingester.class);
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {
            buffer.flushInternal(ingester, true);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        for (Tuple<String, Map<String, Object>> tagAndEvent :
                ImmutableList.of(new Tuple<>("foo", event0), new Tuple<>("bar", event1))) {
            ArgumentCaptor<ByteBuffer> byteBufferArgumentCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
            verify(ingester, times(1)).ingest(eq(tagAndEvent.getFirst()), byteBufferArgumentCaptor.capture());
            ByteBuffer byteBuffer = byteBufferArgumentCaptor.getValue();
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            Map<String, Object> map = objectMapper.readValue(bytes, new TypeReference<Map<String, Object>>() {});
            assertEquals(tagAndEvent.getSecond(), map);
        }
    }

    @Test
    public void testFileBackupThatIsNotDirectory()
            throws IOException
    {
        File backupDirFile = File.createTempFile("testFileBackupWithInvalidDir", ".tmp");
        backupDirFile.deleteOnExit();

        bufferConfig.setFileBackupDir(backupDirFile.getAbsolutePath());

        try (Buffer ignored = new Buffer(bufferConfig, recordFormatter)) {
            fail();
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

        bufferConfig.setFileBackupDir(backupDir.getAbsolutePath());

        try (Buffer ignored = new Buffer(bufferConfig, recordFormatter)) {
            fail();
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

        bufferConfig.setFileBackupDir(backupDir.getAbsolutePath());

        try (Buffer ignored = new Buffer(bufferConfig, recordFormatter)) {
            fail();
        }
        catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testGetAllocatedSize()
            throws IOException
    {
        bufferConfig.setChunkInitialSize(256 * 1024);
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {
            assertThat(buffer.getAllocatedSize(), is(0L));
            Map<String, Object> map = new HashMap<>();
            map.put("name", "komamitsu");
            for (int i = 0; i < 10; i++) {
                buffer.append("foo.bar", new Date().getTime(), map);
            }
            assertThat(buffer.getAllocatedSize(), is(256 * 1024L));
        }
    }

    @Test
    public void testGetBufferedDataSize()
            throws IOException
    {
        bufferConfig.setChunkInitialSize(256 * 1024);
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {
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
    }

    @Test
    public void testAppendIfItDoesNotThrowBufferOverflow()
            throws IOException
    {
        bufferConfig.setChunkInitialSize(64 * 1024);
        try (Buffer buffer = new Buffer(bufferConfig, recordFormatter)) {

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
}