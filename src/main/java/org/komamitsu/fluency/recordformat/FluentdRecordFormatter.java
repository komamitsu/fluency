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
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FluentdRecordFormatter
        extends RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(FluentdRecordFormatter.class);
    private static final Charset CHARSET = Charset.forName("ASCII");
    private final Config config;
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    public FluentdRecordFormatter(Config config)
    {
        super(config.baseConfig);
        this.config = config;
        registerObjectMapperModules(objectMapper);
    }

    @Override
    public byte[] format(String tag, Object timestamp, Map<String, Object> data)
    {
        try {
            return objectMapper.writeValueAsBytes(Arrays.asList(timestamp, data));
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

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len)
    {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // 2 items array
            outputStream.write(0x92);
            objectMapper.writeValue(outputStream, timestamp);
            outputStream.write(mapValue, offset, len);
            outputStream.close();

            return outputStream.toByteArray();
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to convert the record to MessagePack format: cause=%s, tag=%s, timestamp=%s, dataSize=%s",
                            e.getMessage(),
                            tag, timestamp, mapValue.length)
            );
        }
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue)
    {
        int mapValueLen = mapValue.remaining();
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // 2 items array
            outputStream.write(0x92);
            objectMapper.writeValue(outputStream, timestamp);
            while (mapValue.hasRemaining()) {
                outputStream.write(mapValue.get());
            }
            outputStream.close();

            return outputStream.toByteArray();
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
            implements RecordFormatter.Instantiator<FluentdRecordFormatter>
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
        public FluentdRecordFormatter createInstance()
        {
            return new FluentdRecordFormatter(this);
        }
    }
}
