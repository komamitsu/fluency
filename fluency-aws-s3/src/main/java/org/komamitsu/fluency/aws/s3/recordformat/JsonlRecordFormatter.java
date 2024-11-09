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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.komamitsu.fluency.recordformat.AbstractRecordFormatter;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.komamitsu.fluency.recordformat.recordaccessor.MapRecordAccessor;
import org.komamitsu.fluency.recordformat.recordaccessor.MessagePackRecordAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonlRecordFormatter extends AbstractRecordFormatter implements AwsS3RecordFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonlRecordFormatter.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonlRecordFormatter() {
    this(new Config());
  }

  public JsonlRecordFormatter(Config config) {
    super(config);
  }

  private byte[] appendNewLine(String json) {
    return (json + "\n").getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] format(String tag, Object timestamp, Map<String, Object> data) {
    try {
      MapRecordAccessor recordAccessor = new MapRecordAccessor(data);
      recordAccessor.setTimestamp(getEpoch(timestamp));
      return appendNewLine(recordAccessor.toJson(OBJECT_MAPPER));
    } catch (Throwable e) {
      LOG.error(
          String.format(
              "Failed to format a Map record: cause=%s, tag=%s, timestamp=%s, recordCount=%d",
              e.getMessage(), tag, timestamp, data.size()));
      throw e;
    }
  }

  @Override
  public byte[] formatFromMessagePack(
      String tag, Object timestamp, byte[] mapValue, int offset, int len) {
    try {
      MessagePackRecordAccessor recordAccessor =
          new MessagePackRecordAccessor(ByteBuffer.wrap(mapValue, offset, len));
      recordAccessor.setTimestamp(getEpoch(timestamp));
      return appendNewLine(recordAccessor.toJson(OBJECT_MAPPER));
    } catch (Throwable e) {
      LOG.error(
          String.format(
              "Failed to format a MessagePack record: cause=%s, tag=%s, timestamp=%s, offset=%d, len=%d",
              e.getMessage(), tag, timestamp, offset, len));
      throw e;
    }
  }

  @Override
  public byte[] formatFromMessagePack(String tag, Object timestamp, ByteBuffer mapValue) {
    try {
      MessagePackRecordAccessor recordAccessor = new MessagePackRecordAccessor(mapValue);
      recordAccessor.setTimestamp(getEpoch(timestamp));
      return appendNewLine(recordAccessor.toJson(OBJECT_MAPPER));
    } catch (Throwable e) {
      LOG.error(
          String.format(
              "Failed to format a MessagePack record: cause=%s, tag=%s, timestamp=%s, bytebuf=%s",
              e.getMessage(), tag, timestamp, mapValue));
      throw e;
    }
  }

  @Override
  public String formatName() {
    return "jsonl";
  }

  public static class Config extends RecordFormatter.Config {}
}
