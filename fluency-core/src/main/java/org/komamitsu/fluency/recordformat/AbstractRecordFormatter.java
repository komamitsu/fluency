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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.EventTime;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
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
    protected final Config config;

    public AbstractRecordFormatter(Config config)
    {
        this.config = config;
    }

    public abstract byte[] format(String tag, Object timestamp, Map<String, Object> data);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue);

    public abstract String formatName();

    public interface RecordAccessor
    {
        Object get(String key);

        void addTimestamp(long timestamp);

        byte[] toMessagePackByteArray();
    }

    public static class MapRecordAccessor
            implements RecordAccessor
    {
        private final Map<String, Object> map;

        public MapRecordAccessor(Map<String, Object> map)
        {
            this.map = map;
        }

        @Override
        public Object get(String key)
        {
            return map.get(key);
        }

        @Override
        public void addTimestamp(long timestamp)
        {
            map.put("time", timestamp);
        }
    }

    public static class MessagePackRecordAccessor
            implements RecordAccessor
    {
        private static final StringValue KEY_TIME = ValueFactory.newString("time");
        private int bytesLen;
        private Map<Value, Value> map;

        private void updateValue(ByteBuffer byteBuffer)
        {
            bytesLen = byteBuffer.remaining();
            try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(byteBuffer)) {
                this.map = unpacker.unpackValue().asMapValue().map();
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to unpack MessagePack format data", e);
            }
        }

        public MessagePackRecordAccessor(ByteBuffer byteBuffer)
        {
            updateValue(byteBuffer);
        }

        @Override
        public Object get(String key)
        {
            return map.get(ValueFactory.newString(key));
        }

        @Override
        public void addTimestamp(long timestamp)
        {
            int mapSize = map.size();
            try (ByteArrayOutputStream output = new ByteArrayOutputStream(bytesLen + 16)) {
                try (MessagePacker packer = MessagePack.newDefaultPacker(output)) {
                    if (map.containsKey(KEY_TIME)) {
                        packer.packMapHeader(mapSize);
                    }
                    else {
                        packer.packMapHeader(mapSize + 1);
                        KEY_TIME.writeTo(packer);
                        packer.packLong(timestamp);
                    }

                    for (Map.Entry<Value, Value> entry : map.entrySet()) {
                        packer.packValue(entry.getKey());
                        packer.packValue(entry.getValue());
                    }
                }
                updateValue(ByteBuffer.wrap(output.toByteArray()));
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to unpack MessagePack format data", e);
            }
        }
    }

    protected void appendTimeToRecord(Object timestamp, RecordAccessor recordAccessor)
    {
        if (recordAccessor.get("time") == null) {
            long epoch = getEpoch(timestamp);
            recordAccessor.addTimestamp(epoch);
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
