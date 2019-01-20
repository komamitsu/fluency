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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class RecordFormatter
{
    protected final Config config;

    public RecordFormatter(Config config)
    {
        this.config = config;
    }

    protected void registerObjectMapperModules(ObjectMapper objectMapper)
    {
        List<Module> jacksonModules = config.getJacksonModules();
        for (Module module : jacksonModules) {
            objectMapper.registerModule(module);
        }
    }

    public abstract byte[] format(String tag, Object timestamp, Map<String, Object> data);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len);

    public abstract byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue);

    public static class Config
    {
        private List<Module> jacksonModules = Collections.emptyList();

        public List<Module> getJacksonModules()
        {
            return jacksonModules;
        }

        public void setJacksonModules(List<Module> jacksonModules)
        {
            this.jacksonModules = jacksonModules;
        }
    }
}
