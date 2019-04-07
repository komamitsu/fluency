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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class MapRecordAccessor
        implements RecordAccessor
{
    private final ObjectMapper objectMapper;
    private Map<String, Object> map;

    public MapRecordAccessor(ObjectMapper objectMapper, Map<String, Object> map)
    {
        this.map = new HashMap<>(map);
        this.objectMapper = objectMapper;
    }

    @Override
    public String getAsString(String key)
    {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public void setTimestamp(long timestamp)
    {
        map.putIfAbsent("time", timestamp);
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
