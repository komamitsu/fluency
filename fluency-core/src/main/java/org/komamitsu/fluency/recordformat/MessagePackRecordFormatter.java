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

package org.komamitsu.fluency.recordformat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MessagePackRecordFormatter
        extends AbstractRecordFormatter
{
    private static final StringValue KEY_TIME = ValueFactory.newString("time");
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    public MessagePackRecordFormatter()
    {
        this(new Config());
    }

    public MessagePackRecordFormatter(Config config)
    {
        super(config);
        registerObjectMapperModules(objectMapper);
    }

    @Override
    public byte[] format(String tag, Object timestamp, Map<String, Object> data)
    {
        Map<String, Object> record = appendTimeToRecord(timestamp, data);

        try {
            return objectMapper.writeValueAsBytes(record);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to convert the record to MessagePack format: cause=%s, tag=%s, timestamp=%s, recordCount=%d",
                            e.getMessage(),
                            tag, timestamp, data.size())
            );
        }
    }

    private byte[] addTimeColumnToMsgpackRecord(MessageUnpacker unpacker, long timestamp, int mapValueLen)
            throws IOException
    {
        ImmutableMapValue mapValue = unpacker.unpackValue().asMapValue();
        int mapSize = mapValue.size();
        Value[] keyValueArray = mapValue.getKeyValueArray();
        // Find `time` column
        boolean timeColExists = false;
        for (int i = 0; i < mapSize; i++) {
            if (keyValueArray[i * 2].asStringValue().equals(KEY_TIME)) {
                timeColExists = true;
                // TODO: Optimization by returning immediately
                break;
            }
        }

        try (ByteArrayOutputStream output = new ByteArrayOutputStream(mapValueLen + 16)) {
            try (MessagePacker packer = MessagePack.newDefaultPacker(output)) {
                if (timeColExists) {
                    packer.packMapHeader(mapSize);
                }
                else {
                    packer.packMapHeader(mapSize + 1);
                    packer.packString("time");
                    packer.packLong(timestamp);
                }
                for (int i = 0; i < mapSize; i++) {
                    packer.packValue(keyValueArray[i * 2]);
                    packer.packValue(keyValueArray[i * 2 + 1]);
                }
            }
            return output.toByteArray();
        }
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len)
    {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mapValue, offset, len)) {
            return addTimeColumnToMsgpackRecord(unpacker, getEpoch(timestamp), len);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to convert the record to MessagePack format: cause=%s, tag=%s, timestamp=%s, dataSize=%s",
                            e.getMessage(),
                            tag, timestamp, len)
            );
        }
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue)
    {
        // TODO: Optimization
        int mapValueLen = mapValue.remaining();
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mapValue)) {
            return addTimeColumnToMsgpackRecord(unpacker, getEpoch(timestamp), mapValueLen);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to convert the record to MessagePack format: cause=%s, tag=%s, timestamp=%s, dataSize=%s",
                            e.getMessage(),
                            tag, timestamp, mapValueLen)
            );
        }
    }

    @Override
    public String formatName()
    {
        return "msgpack";
    }

    public static class Config
            extends RecordFormatter.Config
    {
    }
}
