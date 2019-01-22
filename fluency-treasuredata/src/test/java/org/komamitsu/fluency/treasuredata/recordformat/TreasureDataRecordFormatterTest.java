package org.komamitsu.fluency.treasuredata.recordformat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TreasureDataRecordFormatterTest
{
    private static final String TAG = "foodb.bartbl";
    private static final long FUTURE_EPOCH = 4294967296L;
    private static final Map<String, Object> RECORD_0 = ImmutableMap.of("name", "first", "age", 42, "email", "hello@world.com");
    private static final Map<String, Object> RECORD_1 = ImmutableMap.of("name", "second", "age", 55, "time", FUTURE_EPOCH, "comment", "zzzzzz");
    private static final Map<String, Object> RECORD_2 = ImmutableMap.of("job", "knight", "name", "third", "age", 99);
    private static final StringValue KEY_TIME = ValueFactory.newString("time");
    private static final StringValue KEY_NAME = ValueFactory.newString("name");
    private static final StringValue KEY_AGE = ValueFactory.newString("age");
    private static final StringValue KEY_EMAIL = ValueFactory.newString("email");
    private static final StringValue KEY_COMMENT = ValueFactory.newString("comment");
    private static final StringValue KEY_JOB = ValueFactory.newString("job");
    private TreasureDataRecordFormatter recordFormatter;

    @BeforeEach
    void setUp()
    {
        recordFormatter = new TreasureDataRecordFormatter(new TreasureDataRecordFormatter.Config());
    }

    private void assertRecord0(byte[] formatted, long expectedTime)
            throws IOException
    {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(formatted)) {
            Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
            assertEquals(4, map.size());
            assertEquals(expectedTime, map.get(KEY_TIME).asNumberValue().toLong());
            assertEquals("first", map.get(KEY_NAME).asStringValue().asString());
            assertEquals(42, map.get(KEY_AGE).asNumberValue().toInt());
            assertEquals("hello@world.com", map.get(KEY_EMAIL).asStringValue().asString());
        }
    }

    private void assertRecord1(byte[] formatted, long expectedTime)
            throws IOException
    {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(formatted)) {
            Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
            assertEquals(4, map.size());
            assertEquals(expectedTime, map.get(KEY_TIME).asNumberValue().toLong());
            assertEquals("second", map.get(KEY_NAME).asStringValue().asString());
            assertEquals(55, map.get(KEY_AGE).asNumberValue().toInt());
            assertEquals("zzzzzz", map.get(KEY_COMMENT).asStringValue().asString());
        }
    }

    private void assertRecord2(byte[] formatted, long expectedTime)
            throws IOException
    {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(formatted)) {
            Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
            assertEquals(4, map.size());
            assertEquals(expectedTime, map.get(KEY_TIME).asNumberValue().toLong());
            assertEquals("third", map.get(KEY_NAME).asStringValue().asString());
            assertEquals(99, map.get(KEY_AGE).asNumberValue().toInt());
            assertEquals("knight", map.get(KEY_JOB).asStringValue().asString());
        }
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
