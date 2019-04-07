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

package org.komamitsu.fluency.recordformat.recordaccessor;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MessagePackRecordAccessor
        implements RecordAccessor
{
    private static final StringValue KEY_TIME = ValueFactory.newString("time");
    private MapValueBytes mapValueBytes;

    private static class MapValueBytes {
        private final byte[] byteArray;
        private final Map<Value, Value> map;

        MapValueBytes(ByteBuffer byteBuffer)
        {
            byte[] mapValueBytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(mapValueBytes);
            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mapValueBytes)) {
                this.map = unpacker.unpackValue().asMapValue().map();
                this.byteArray = mapValueBytes;
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Invalid MessagePack ByteBuffer", e);
            }
        }

        byte[] byteArray()
        {
            return byteArray;
        }

        Map<Value, Value> map()
        {
            return map;
        }
    }

    public MessagePackRecordAccessor(ByteBuffer byteBuffer)
    {
        mapValueBytes = new MapValueBytes(byteBuffer);
    }

    @Override
    public String getAsString(String key)
    {
        Value value = mapValueBytes.map().get(ValueFactory.newString(key));
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public void setTimestamp(long timestamp)
    {
        int mapSize = mapValueBytes.map().size();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream(mapValueBytes.byteArray().length + 16)) {
            try (MessagePacker packer = MessagePack.newDefaultPacker(output)) {
                if (mapValueBytes.map().containsKey(KEY_TIME)) {
                    packer.packMapHeader(mapSize);
                }
                else {
                    packer.packMapHeader(mapSize + 1);
                    KEY_TIME.writeTo(packer);
                    packer.packLong(timestamp);
                }

                for (Map.Entry<Value, Value> entry : mapValueBytes.map().entrySet()) {
                    packer.packValue(entry.getKey());
                    packer.packValue(entry.getValue());
                }
            }
            mapValueBytes = new MapValueBytes(ByteBuffer.wrap(output.toByteArray()));
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to upsert `time` field", e);
        }
    }

    @Override
    public byte[] toMessagePackByteArray()
    {
        return mapValueBytes.byteArray();
    }
}
