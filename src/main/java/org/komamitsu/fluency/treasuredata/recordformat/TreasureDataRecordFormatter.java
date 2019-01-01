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

package org.komamitsu.fluency.treasuredata.recordformat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TreasureDataRecordFormatter
        extends RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(TreasureDataRecordFormatter.class);
    private final Config config;
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    public TreasureDataRecordFormatter(Config config)
    {
        super(config.baseConfig);
        this.config = config;
        registerObjectMapperModules(objectMapper);
    }

    private long getEpoch(Object timestamp)
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

    @Override
    public byte[] format(String tag, Object timestamp, Map<String, Object> data)
    {
        HashMap<Object, Object> record = new HashMap<>(data);
        long epoch = getEpoch(timestamp);
        record.put("time", epoch);

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

    private byte[] addTimeColumnToMsgpackRecord(String tag, MessageUnpacker unpacker, long timestamp, int mapValueLen)
            throws IOException
    {
        // TODO: Optimization
        Map<Value, Value> map = unpacker.unpackValue().asMapValue().map();
        map.put(ValueFactory.newString("time"), ValueFactory.newInteger(timestamp));

        ByteArrayOutputStream output = new ByteArrayOutputStream(mapValueLen + 16);
        MessagePacker packer = MessagePack.newDefaultPacker(output);

        ValueFactory.newMap(map).writeTo(packer);
        packer.close();

        return output.toByteArray();
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len)
    {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mapValue, offset, len)) {
            return addTimeColumnToMsgpackRecord(tag, unpacker, getEpoch(timestamp), len);
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
            return addTimeColumnToMsgpackRecord(tag, unpacker, getEpoch(timestamp), mapValueLen);
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

    public static class Config
            implements RecordFormatter.Instantiator<TreasureDataRecordFormatter>
    {
        RecordFormatter.Config baseConfig = new RecordFormatter.Config();

        public List<Module> getJacksonModules()
        {
            return baseConfig.getJacksonModules();
        }

        public Config setJacksonModules(List<Module> jacksonModules)
        {
            baseConfig.setJacksonModules(jacksonModules);
            return this;
        }

        @Override
        public TreasureDataRecordFormatter createInstance()
        {
            return new TreasureDataRecordFormatter(this);
        }
    }
}
