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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.EventTime;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TreasureDataRecordFormatter
        implements RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(TreasureDataRecordFormatter.class);
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

    @Override
    public byte[] format(String tag, Object timestamp, Map<String, Object> data)
    {
        HashMap<Object, Object> record = new HashMap<>(data);
        long epoch;
        if (timestamp instanceof EventTime) {
            epoch = ((EventTime) timestamp).getSeconds();
        }
        else if (timestamp instanceof Number) {
            epoch = ((Number) timestamp).longValue();
        }
        else {
            LOG.warn("Invalid timestamp. Using current time: timestamp={}", timestamp);
            epoch = System.currentTimeMillis() / 1000;
        }
        record.put("time", epoch);

        try {
            return objectMapper.writeValueAsBytes(record);
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to convert the record to MessagePack format: cause=%s, timestamp=%s, data=%s",
                            e.getMessage(),
                            timestamp, data)
            );
        }
    }
}
