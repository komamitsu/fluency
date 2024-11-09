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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.msgpack.jackson.dataformat.MessagePackFactory;

class CsvRecordFormatterTest {
  private static final String TAG = "foodb.bartbl";
  private static final long FUTURE_EPOCH = 4294967296L;
  private static final Map<String, Object> RECORD_0 =
      ImmutableMap.of("name", "first", "age", 42, "email", "hello@world.com");
  private static final Map<String, Object> RECORD_1 =
      ImmutableMap.of("name", "second", "age", 55, "time", FUTURE_EPOCH, "comment", "zzzzzz");
  private static final Map<String, Object> RECORD_2 =
      ImmutableMap.of("job", "knight", "name", "third", "age", 99, "ignored", "foobar");
  private static final Consumer<CsvRecordFormatter.Config> CONFIGURATOR_NORMAL = config -> {};
  private static final Consumer<CsvRecordFormatter.Config> CONFIGURATOR_QUOTE =
      config -> config.setQuote("\"");
  private static final Consumer<CsvRecordFormatter.Config> CONFIGURATOR_TAB_DELIM =
      config -> config.setDelimiter("\t");
  private static final Consumer<CsvRecordFormatter.Config> CONFIGURATOR_QUOTE_TAB_DELIM =
      config -> {
        config.setQuote("\"");
        config.setDelimiter("\t");
      };
  private static final Function<Object, String> QUOTER_NOTHING = x -> x == null ? "" : x.toString();
  private static final Function<Object, String> QUOTER_QUOTE =
      x -> x == null ? "" : "\"" + x + "\"";
  private CsvRecordFormatter.Config config;

  enum Option {
    NORMAL("%s,%s,%s,%s,,%s,%s\n", CONFIGURATOR_NORMAL, QUOTER_NOTHING),
    QUOTE("%s,%s,%s,%s,,%s,%s\n", CONFIGURATOR_QUOTE, QUOTER_QUOTE),
    TAB_DELIM("%s\t%s\t%s\t%s\t\t%s\t%s\n", CONFIGURATOR_TAB_DELIM, QUOTER_NOTHING),
    QUOTE_AND_TAB_DELIM("%s\t%s\t%s\t%s\t\t%s\t%s\n", CONFIGURATOR_QUOTE_TAB_DELIM, QUOTER_QUOTE);

    private final String format;
    private final Consumer<CsvRecordFormatter.Config> configurator;
    private final Function<Object, String> quoter;

    Option(
        String format,
        Consumer<CsvRecordFormatter.Config> configurator,
        Function<Object, String> quoter) {
      this.format = format;
      this.configurator = configurator;
      this.quoter = quoter;
    }
  }

  @BeforeEach
  void setUp() {
    config = new CsvRecordFormatter.Config();
    config.setColumnNames(
        ImmutableList.of("time", "name", "age", "email", "none", "comment", "job"));
  }

  private void assertRecord0(byte[] formatted, long expectedTime, Option option) {
    assertEquals(
        String.format(
            option.format,
            option.quoter.apply(expectedTime), // time
            option.quoter.apply("first"), // name
            option.quoter.apply(42), // age
            option.quoter.apply("hello@world.com"), // email
            option.quoter.apply(null), // comment
            option.quoter.apply(null) // job
            ),
        new String(formatted, StandardCharsets.UTF_8));
  }

  private void assertRecord1(byte[] formatted, long expectedTime, Option option) {
    assertEquals(
        String.format(
            option.format,
            option.quoter.apply(expectedTime), // time
            option.quoter.apply("second"), // name
            option.quoter.apply(55), // age
            option.quoter.apply(null), // email
            option.quoter.apply("zzzzzz"), // comment
            option.quoter.apply(null) // job
            ),
        new String(formatted, StandardCharsets.UTF_8));
  }

  private void assertRecord2(byte[] formatted, long expectedTime, Option option) {
    assertEquals(
        String.format(
            option.format,
            option.quoter.apply(expectedTime), // time
            option.quoter.apply("third"), // name
            option.quoter.apply(99), // age
            option.quoter.apply(null), // email
            option.quoter.apply(null), // comment
            option.quoter.apply("knight") // job
            ),
        new String(formatted, StandardCharsets.UTF_8));
  }

  @ParameterizedTest
  @EnumSource(Option.class)
  void format(Option option) {
    option.configurator.accept(config);
    CsvRecordFormatter recordFormatter = new CsvRecordFormatter(config);
    long now = System.currentTimeMillis() / 1000;
    assertRecord0(recordFormatter.format(TAG, now, RECORD_0), now, option);
    assertRecord1(recordFormatter.format(TAG, now, RECORD_1), FUTURE_EPOCH, option);
    assertRecord2(recordFormatter.format(TAG, now, RECORD_2), now, option);
  }

  @ParameterizedTest
  @EnumSource(Option.class)
  void formatFromMessagePackBytes(Option option) throws IOException {
    option.configurator.accept(config);
    CsvRecordFormatter recordFormatter = new CsvRecordFormatter(config);
    long now = System.currentTimeMillis() / 1000;
    ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    {
      byte[] bytes = objectMapper.writeValueAsBytes(RECORD_0);
      assertRecord0(
          recordFormatter.formatFromMessagePack(TAG, now, bytes, 0, bytes.length), now, option);
    }
    {
      byte[] bytes = objectMapper.writeValueAsBytes(RECORD_1);
      assertRecord1(
          recordFormatter.formatFromMessagePack(TAG, now, bytes, 0, bytes.length),
          FUTURE_EPOCH,
          option);
    }
    {
      byte[] bytes = objectMapper.writeValueAsBytes(RECORD_2);
      assertRecord2(
          recordFormatter.formatFromMessagePack(TAG, now, bytes, 0, bytes.length), now, option);
    }
  }

  private ByteBuffer convertMapToMessagePackByteBuffer(Map<String, Object> record, boolean isDirect)
      throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    byte[] bytes = objectMapper.writeValueAsBytes(record);
    ByteBuffer byteBuffer;
    if (isDirect) {
      byteBuffer = ByteBuffer.allocateDirect(bytes.length);
    } else {
      byteBuffer = ByteBuffer.allocate(bytes.length);
    }
    byteBuffer.put(bytes);
    byteBuffer.flip();
    return byteBuffer;
  }

  @ParameterizedTest
  @EnumSource(Option.class)
  void formatFromMessagePackByteBuffer(Option option) throws IOException {
    option.configurator.accept(config);
    CsvRecordFormatter recordFormatter = new CsvRecordFormatter(config);
    long now = System.currentTimeMillis() / 1000;
    assertRecord0(
        recordFormatter.formatFromMessagePack(
            TAG, now, convertMapToMessagePackByteBuffer(RECORD_0, false)),
        now,
        option);
    assertRecord1(
        recordFormatter.formatFromMessagePack(
            TAG, now, convertMapToMessagePackByteBuffer(RECORD_1, true)),
        FUTURE_EPOCH,
        option);
    assertRecord2(
        recordFormatter.formatFromMessagePack(
            TAG, now, convertMapToMessagePackByteBuffer(RECORD_2, true)),
        now,
        option);
  }
}
