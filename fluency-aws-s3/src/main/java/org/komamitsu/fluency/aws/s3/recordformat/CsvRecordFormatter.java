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

package org.komamitsu.fluency.aws.s3.recordformat;

import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableMapValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CsvRecordFormatter
        extends AwsS3RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(CsvRecordFormatter.class);
    private static final String KEY_TIME = "time";
    private final Config config;
    private final byte[] delimiter;
    private final byte[] quote;
    private final byte[] lineBreak;

    public CsvRecordFormatter()
    {
        this(new Config());
    }

    public CsvRecordFormatter(Config config)
    {
        super(config);
        config.validateValues();
        this.config = config;

        String delimiterInConfig = config.getDelimiter();
        if (delimiterInConfig == null) {
            delimiter = null;
        }
        else {
            delimiter = delimiterInConfig.getBytes(StandardCharsets.UTF_8);
        }

        String quoteInConfig = config.getQuote();
        if (quoteInConfig == null) {
            quote = null;
        }
        else {
            quote = quoteInConfig.getBytes(StandardCharsets.UTF_8);
        }

        lineBreak = new byte[] { 0x0A };
    }

    // TODO: Make this configurable (e.g. nano time)
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
        Map<String, Object> record;
        if (data.get("time") == null) {
            record = new HashMap<>(data);
            long epoch = getEpoch(timestamp);
            record.put("time", epoch);
        }
        else {
            record = data;
        }

        boolean isFirst = true;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (String columnName : config.getColumnNames()) {
            if (delimiter != null) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    output.write(delimiter, 0, delimiter.length);
                }
            }

            Object value = record.get(columnName);
            if (value == null) {
                continue;
            }

            byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);

            if (quote == null) {
                output.write(bytes, 0, bytes.length);
            }
            else {
                output.write(quote, 0, quote.length);
                output.write(bytes, 0, bytes.length);
                output.write(quote, 0, quote.length);
            }
        }
        output.write(lineBreak, 0, lineBreak.length);

        return output.toByteArray();
    }

    private byte[] addTimeColumnToMsgpackRecord(MessageUnpacker unpacker, long timestamp, int mapValueLen)
            throws IOException
    {
        ImmutableMapValue mapValue = unpacker.unpackValue().asMapValue();
        int mapSize = mapValue.size();
        Value[] keyValueArray = mapValue.getKeyValueArray();
        // Find `time` column
        boolean timeColExists = false;
        for (int i = 0; i < mapSize; i++) {
            if (keyValueArray[i * 2].asStringValue().equals(KEY_TIME)) {
                timeColExists = true;
                // TODO: Optimization by returning immediately
                break;
            }
        }

        try (ByteArrayOutputStream output = new ByteArrayOutputStream(mapValueLen + 16)) {
            try (MessagePacker packer = MessagePack.newDefaultPacker(output)) {
                if (timeColExists) {
                    packer.packMapHeader(mapSize);
                }
                else {
                    packer.packMapHeader(mapSize + 1);
                    packer.packString("time");
                    packer.packLong(timestamp);
                }
                for (int i = 0; i < mapSize; i++) {
                    packer.packValue(keyValueArray[i * 2]);
                    packer.packValue(keyValueArray[i * 2 + 1]);
                }
            }
            return output.toByteArray();
        }
    }

    @Override
    public byte[] formatFromMessagePack(String tag, Object timestamp, byte[] mapValue, int offset, int len)
    {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mapValue, offset, len)) {
            return addTimeColumnToMsgpackRecord(unpacker, getEpoch(timestamp), len);
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
            return addTimeColumnToMsgpackRecord(unpacker, getEpoch(timestamp), mapValueLen);
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

    @Override
    public String formatName()
    {
        return "csv";
    }

    public static class Config
            extends RecordFormatter.Config
    {
        private List<String> columnNames;
        private String quoteString;
        private String delimiter = ",";

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public void setColumnNames(List<String> columnNames)
        {
            this.columnNames = columnNames;
        }

        public String getQuote()
        {
            return quoteString;
        }

        public void setQuoteString(String quoteString)
        {
            this.quoteString = quoteString;
        }

        public String getDelimiter()
        {
            return delimiter;
        }

        public void setDelimiter(String delimiter)
        {
            this.delimiter = delimiter;
        }

        public void validateValues()
        {
            if (columnNames == null || columnNames.isEmpty()) {
                throw new IllegalArgumentException("Column names should be empty");
            }
        }
    }
}
