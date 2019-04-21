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
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class AbstractRecordFormatter
        implements RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRecordFormatter.class);
    protected final ObjectMapper objectMapperForMessagePack = new ObjectMapper(new MessagePackFactory());
    protected final Config config;

    public AbstractRecordFormatter(Config config)
    {
        this.config = config;
        registerObjectMapperModules(objectMapperForMessagePack);
    }

    public abstract byte[] format(String tag, Object timestamp, Map<String, Object> data);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue);

    public abstract String formatName();

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
