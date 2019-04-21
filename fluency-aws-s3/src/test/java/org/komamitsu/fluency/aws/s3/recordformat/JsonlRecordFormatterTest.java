/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.aws.s3.recordformat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonlRecordFormatterTest
{
    private static final String TAG = "foodb.bartbl";
    private static final long FUTURE_EPOCH = 4294967296L;
    private static final Map<String, Object> RECORD_0 = ImmutableMap.of("name", "first", "age", 42, "email", "hello@world.com");
    private static final Map<String, Object> RECORD_1 = ImmutableMap.of("name", "second", "age", 55, "time", FUTURE_EPOCH, "comment", "zzzzzz");
    private static final Map<String, Object> RECORD_2 = ImmutableMap.of("job", "knight", "name", "third", "age", 99);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private JsonlRecordFormatter recordFormatter;

    @BeforeEach
    void setUp()
    {
        recordFormatter = new JsonlRecordFormatter(new JsonlRecordFormatter.Config());
    }

    private void assertRecord0(byte[] formatted, long expectedTime)
            throws IOException
    {
        JsonNode jsonNode = objectMapper.readTree(formatted);
        assertTrue(jsonNode.isObject());
        assertEquals(4, jsonNode.size());
        assertEquals(expectedTime, jsonNode.get("time").asLong());
        assertEquals("first", jsonNode.get("name").asText());
        assertEquals(42, jsonNode.get("age").asInt());
        assertEquals("hello@world.com", jsonNode.get("email").asText());
    }

    private void assertRecord1(byte[] formatted, long expectedTime)
            throws IOException
    {
        JsonNode jsonNode = objectMapper.readTree(formatted);
        assertTrue(jsonNode.isObject());
        assertEquals(4, jsonNode.size());
        assertEquals(expectedTime, jsonNode.get("time").asLong());
        assertEquals("second", jsonNode.get("name").asText());
        assertEquals(55, jsonNode.get("age").asInt());
        assertEquals("zzzzzz", jsonNode.get("comment").asText());
    }

    private void assertRecord2(byte[] formatted, long expectedTime)
            throws IOException
    {
        JsonNode jsonNode = objectMapper.readTree(formatted);
        assertTrue(jsonNode.isObject());
        assertEquals(4, jsonNode.size());
        assertEquals(expectedTime, jsonNode.get("time").asLong());
        assertEquals("third", jsonNode.get("name").asText());
        assertEquals(99, jsonNode.get("age").asInt());
        assertEquals("knight", jsonNode.get("job").asText());
    }

    @Test
    void format()
            throws IOException
    {
        long now = System.currentTimeMillis() / 1000;
        assertRecord0(recordFormatter.format(TAG, now, RECORD_0), now);
        assertRecord1(recordFormatter.format(TAG, now, RECORD_1), FUTURE_EPOCH);
        assertRecord2(recordFormatter.format(TAG, now, RECORD_2), now);
    }

    @Test
    void formatFromMessagePackBytes()
            throws IOException
    {
        long now = System.currentTimeMillis() / 1000;
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        {
            byte[] bytes = objectMapper.writeValueAsBytes(RECORD_0);
            assertRecord0(recordFormatter.formatFromMessagePack(TAG, now, bytes, 0, bytes.length), now);
        }
        {
            byte[] bytes = objectMapper.writeValueAsBytes(RECORD_1);
            assertRecord1(recordFormatter.formatFromMessagePack(TAG, now, bytes, 0, bytes.length), FUTURE_EPOCH);
        }
        {
            byte[] bytes = objectMapper.writeValueAsBytes(RECORD_2);
            assertRecord2(recordFormatter.formatFromMessagePack(TAG, now, bytes, 0, bytes.length), now);
        }
    }

    private ByteBuffer convertMapToMessagePackByteBuffer(Map<String, Object> record, boolean isDirect)
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        byte[] bytes = objectMapper.writeValueAsBytes(record);
        ByteBuffer byteBuffer;
        if (isDirect) {
            byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        }
        else {
            byteBuffer = ByteBuffer.allocate(bytes.length);
        }
        byteBuffer.put(bytes);
        byteBuffer.flip();
        return byteBuffer;
    }

    @Test
    void formatFromMessagePackByteBuffer()
            throws IOException
    {
        long now = System.currentTimeMillis() / 1000;
        assertRecord0(recordFormatter.formatFromMessagePack(TAG, now, convertMapToMessagePackByteBuffer(RECORD_0, false)), now);
        assertRecord1(recordFormatter.formatFromMessagePack(TAG, now, convertMapToMessagePackByteBuffer(RECORD_1, true)), FUTURE_EPOCH);
        assertRecord2(recordFormatter.formatFromMessagePack(TAG, now, convertMapToMessagePackByteBuffer(RECORD_2, true)), now);
    }
}