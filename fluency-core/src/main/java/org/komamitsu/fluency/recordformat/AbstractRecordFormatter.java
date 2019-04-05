/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.recordformat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.EventTime;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class AbstractRecordFormatter
        implements RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRecordFormatter.class);
    protected final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    protected final Config config;

    public AbstractRecordFormatter(Config config)
    {
        this.config = config;
        registerObjectMapperModules(objectMapper);
    }

    public abstract byte[] format(String tag, Object timestamp, Map<String, Object> data);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue);

    public abstract String formatName();

    public interface RecordAccessor
    {
        String getAsString(String key);

        void setTimestamp(long timestamp);

        byte[] toMessagePackByteArray();
    }

    public static class MapRecordAccessor
            implements RecordAccessor
    {
        private final ObjectMapper objectMapper;
        private final Map<String, Object> map;

        public MapRecordAccessor(ObjectMapper objectMapper, Map<String, Object> map)
        {
            this.map = map;
            this.objectMapper = objectMapper;
        }

        @Override
        public String getAsString(String key)
        {
            return map.get(key).toString();
        }

        @Override
        public void setTimestamp(long timestamp)
        {
            map.put("time", timestamp);
        }

        @Override
        public byte[] toMessagePackByteArray()
        {
            try {
                return objectMapper.writeValueAsBytes(map);
            }
            catch (JsonProcessingException e) {
                throw new IllegalStateException("Failed to serialize the map to MessagePack format", e);
            }
        }
    }

    public static class MessagePackRecordAccessor
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
            return mapValueBytes.map().get(ValueFactory.newString(key)).toString();
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

    protected long getEpoch(Object timestamp)
    {
        if (timestamp instanceof EventTime) {
            return ((EventTime) timestamp).getSeconds();
        }
        else if (timestamp instanceof Number) {
            return ((Number) timestamp).longValue();
        }
        else {
            LOG.warn("Invalid timestamp. Using current time: timestamp={}", timestamp);
            return System.currentTimeMillis() / 1000;
        }
    }

    protected void registerObjectMapperModules(ObjectMapper objectMapper)
    {
        List<Module> jacksonModules = config.getJacksonModules();
        for (Module module : jacksonModules) {
            objectMapper.registerModule(module);
        }
    }
}
