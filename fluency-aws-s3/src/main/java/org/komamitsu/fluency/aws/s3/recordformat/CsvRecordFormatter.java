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

import org.komamitsu.fluency.recordformat.AbstractRecordFormatter;
import org.komamitsu.fluency.recordformat.recordaccessor.MapRecordAccessor;
import org.komamitsu.fluency.recordformat.recordaccessor.MessagePackRecordAccessor;
import org.komamitsu.fluency.recordformat.recordaccessor.RecordAccessor;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class CsvRecordFormatter
        extends AbstractRecordFormatter
        implements AwsS3RecordFormatter
{
    private static final Logger LOG = LoggerFactory.getLogger(CsvRecordFormatter.class);
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

    private byte[] formatInternal(RecordAccessor recordAccessor)
    {
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

            String value = recordAccessor.getAsString(columnName);
            if (value == null) {
                continue;
            }

            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

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

    @Override
    public byte[] format(String tag, Object timestamp, Map<String, Object> data)
    {
        try {
            RecordAccessor recordAccessor = new MapRecordAccessor(data);
            recordAccessor.setTimestamp(getEpoch(timestamp));
            return formatInternal(recordAccessor);
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
            RecordAccessor recordAccessor = new MessagePackRecordAccessor(ByteBuffer.wrap(mapValue, offset, len));
            recordAccessor.setTimestamp(getEpoch(timestamp));
            return formatInternal(recordAccessor);
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
            RecordAccessor recordAccessor = new MessagePackRecordAccessor(mapValue);
            recordAccessor.setTimestamp(getEpoch(timestamp));
            return formatInternal(recordAccessor);
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

        public void setQuote(String quoteString)
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
