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

import org.komamitsu.fluency.recordformat.recordaccessor.MapRecordAccessor;
import org.komamitsu.fluency.recordformat.recordaccessor.MessagePackRecordAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class MessagePackRecordFormatter
        extends AbstractRecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(MessagePackRecordFormatter.class);

    public MessagePackRecordFormatter()
    {
        this(new Config());
    }

    public MessagePackRecordFormatter(Config config)
    {
        super(config);
    }

    @Override
    public byte[] format(String tag, Object timestamp, Map<String, Object> data)
    {
        try {
            MapRecordAccessor recordAccessor = new MapRecordAccessor(data);
            recordAccessor.setTimestamp(getEpoch(timestamp));
            return recordAccessor.toMessagePackByteArray(objectMapperForMessagePack);
        }
        catch (Throwable e) {
            LOG.error(String.format(
                    "Failed to format a Map record: cause=%s, tag=%s, timestamp=%s, recordCount=%d",
                    e.getMessage(), tag, timestamp, data.size()));
            throw e;
        }
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len)
    {
        try {
            MessagePackRecordAccessor recordAccessor = new MessagePackRecordAccessor(ByteBuffer.wrap(mapValue, offset, len));
            recordAccessor.setTimestamp(getEpoch(timestamp));
            return recordAccessor.toMessagePackByteArray(objectMapperForMessagePack);
        }
        catch (Throwable e) {
            LOG.error(String.format(
                    "Failed to format a MessagePack record: cause=%s, tag=%s, timestamp=%s, offset=%d, len=%d",
                    e.getMessage(), tag, timestamp, offset, len));
            throw e;
        }
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue)
    {
        try {
            MessagePackRecordAccessor recordAccessor = new MessagePackRecordAccessor(mapValue);
            recordAccessor.setTimestamp(getEpoch(timestamp));
            return recordAccessor.toMessagePackByteArray(objectMapperForMessagePack);
        }
        catch (Throwable e) {
            LOG.error(String.format(
                    "Failed to format a MessagePack record: cause=%s, tag=%s, timestamp=%s, bytebuf=%s",
                    e.getMessage(), tag, timestamp, mapValue));
            throw e;
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
