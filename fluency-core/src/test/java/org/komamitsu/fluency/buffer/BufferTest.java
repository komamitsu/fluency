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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.JsonRecordFormatter;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.komamitsu.fluency.util.Tuple;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BufferTest
{
    private static final Logger LOG = LoggerFactory.getLogger(BufferTest.class);
    private Ingester ingester;
    private RecordFormatter recordFormatter;
    private Buffer.Config bufferConfig;

    @BeforeEach
    void setUp()
    {
        recordFormatter = new JsonRecordFormatter();
        ingester = mock(Ingester.class);
        bufferConfig = new Buffer.Config();
    }

    @Test
    void testBuffer()
            throws IOException
    {
        int recordSize = 20; // '{"name":"komamitsu"}'
        int tags = 10;
        float allocRatio = 2f;
        int maxAllocSize = (recordSize * 4) * tags;

        bufferConfig.setChunkInitialSize(recordSize);
        bufferConfig.setChunkExpandRatio(allocRatio);
        bufferConfig.setMaxBufferSize(maxAllocSize);
        bufferConfig.setChunkRetentionSize(maxAllocSize / 2);
        Buffer buffer = new Buffer(bufferConfig, recordFormatter);

        assertEquals(0, buffer.getAllocatedSize());
        assertEquals(0, buffer.getBufferedDataSize());
        assertEquals(0f, buffer.getBufferUsage(), 0.001);

        Map<String, Object> data = new HashMap<>();
        data.put("name", "komamitsu");
        for (int i = 0; i < tags; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        assertEquals(recordSize * tags, buffer.getAllocatedSize());
        assertEquals(recordSize * tags, buffer.getBufferedDataSize());
        assertEquals(((float) recordSize) * tags / maxAllocSize, buffer.getBufferUsage(), 0.001);
        for (int i = 0; i < tags; i++) {
            buffer.append("foodb.bartbl" + i, 1420070400, data);
        }
        float totalAllocatedSize = recordSize * (1 + allocRatio) * tags;
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
    void testStrictBufferRetentionSizeManagement()
            throws IOException
    {
        int recordSize = 20; // '{"name":"komamitsu"}'
        int initSize = 15;
        float allocRatio = 2f;
        // 15 -> 30 -> 60 -> 120 -> 240
        int maxAllocSize = (int) (initSize * allocRatio * allocRatio * allocRatio * allocRatio);
        String tag = "foodb.bartbl";

        int retentionSize = 60;

        bufferConfig.setChunkInitialSize(initSize);
        bufferConfig.setChunkExpandRatio(allocRatio);
        bufferConfig.setMaxBufferSize(maxAllocSize);
        bufferConfig.setChunkRetentionSize(retentionSize);
        Buffer buffer = new Buffer(bufferConfig, recordFormatter);

        Map<String, Object> data = new HashMap<>();
        data.put("name", "komamitsu");

        // buffered size: 20
        buffer.append(tag, 1420070400, data);
        buffer.flush(ingester, false);
        verify(ingester, times(0)).ingest(anyString(), any(ByteBuffer.class));

        // buffered size: 40
        buffer.append(tag, 1420070400, data);
        buffer.flush(ingester, false);
        verify(ingester, times(0)).ingest(anyString(), any(ByteBuffer.class));

        // buffered size: 60
        buffer.append(tag, 1420070400, data);
        buffer.flush(ingester, false);
        verify(ingester, times(0)).ingest(anyString(), any(ByteBuffer.class));

        // buffered size: 80
        doAnswer(invocation -> {
            String receivedTag = invocation.getArgument(0);
            ByteBuffer receivedBuffer = invocation.getArgument(1);
            assertEquals(tag, receivedTag);
            assertEquals(60, receivedBuffer.remaining());
            return null;
        }).when(ingester).ingest(anyString(), any(ByteBuffer.class));

        buffer.append(tag, 1420070400, data);
        buffer.flush(ingester, false);
        verify(ingester, times(1)).ingest(anyString(), any(ByteBuffer.class));
    }

    @Test
    void flush()
            throws IOException
    {
        Buffer buffer = new Buffer(bufferConfig, recordFormatter);

        Ingester ingester = mock(Ingester.class);
        buffer.flush(ingester, false);

        verify(ingester, times(0)).ingest(anyString(), any(ByteBuffer.class));

        Map<String, Object> data = new HashMap<>();
        data.put("name", "komamitsu");
        buffer.append("foodb.bartbl", 1420070400, data);

        buffer.flush(ingester, false);

        verify(ingester, times(0)).ingest(anyString(), any(ByteBuffer.class));

        buffer.flush(ingester, true);

        verify(ingester, times(1)).ingest(eq("foodb.bartbl"), any(ByteBuffer.class));
    }

    @Test
    void testFileBackup()
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
    void testFileBackupThatIsNotDirectory()
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
    void testFileBackupThatIsWritable()
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
    void testFileBackupThatIsReadable()
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
    void testGetAllocatedSize()
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
    void testGetBufferedDataSize()
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
    void testAppendIfItDoesNotThrowBufferOverflow()
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

    @Test
    void validateConfig()
    {
        {
            Buffer.Config config = new Buffer.Config();
            config.setChunkInitialSize(4 * 1024 * 1024);
            config.setChunkRetentionSize(4 * 1024 * 1024);
            assertThrows(IllegalArgumentException.class, () -> new Buffer(config, recordFormatter));
        }

        {
            Buffer.Config config = new Buffer.Config();
            config.setChunkRetentionSize(64 * 1024 * 1024);
            config.setMaxBufferSize(64 * 1024 * 1024);
            assertThrows(IllegalArgumentException.class, () -> new Buffer(config, recordFormatter));
        }

        {
            Buffer.Config config = new Buffer.Config();
            config.setChunkExpandRatio(1.19f);
            assertThrows(IllegalArgumentException.class, () -> new Buffer(config, recordFormatter));
        }

        {
            Buffer.Config config = new Buffer.Config();
            config.setChunkRetentionTimeMillis(49);
            assertThrows(IllegalArgumentException.class, () -> new Buffer(config, recordFormatter));
        }
    }
}
