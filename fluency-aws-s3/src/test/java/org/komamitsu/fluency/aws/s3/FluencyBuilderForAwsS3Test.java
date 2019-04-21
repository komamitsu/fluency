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

package org.komamitsu.fluency.aws.s3;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.aws.s3.ingester.AwsS3Ingester;
import org.komamitsu.fluency.aws.s3.ingester.DefaultS3DestinationDecider;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import org.komamitsu.fluency.aws.s3.recordformat.AwsS3RecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.CsvRecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.JsonlRecordFormatter;
import org.komamitsu.fluency.aws.s3.recordformat.MessagePackRecordFormatter;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.regions.Region;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static java.time.ZoneId.SHORT_IDS;
import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FluencyBuilderForAwsS3Test
{
    private FluencyBuilderForAwsS3 builderWithDefaultConfig;
    private FluencyBuilderForAwsS3 builderWithCustomConfig;
    private Fluency fluency;

    @BeforeEach
    void setUp()
    {
        fluency = mock(Fluency.class);

        builderWithDefaultConfig = spy(new FluencyBuilderForAwsS3());
        builderWithDefaultConfig.setFormatCsvColumnNames(ImmutableList.of("dummy"));
        doReturn(fluency).when(builderWithDefaultConfig)
                .createFluency(
                        any(RecordFormatter.class),
                        any(Ingester.class),
                        any(Buffer.Config.class),
                        any(Flusher.Config.class));

        {
            FluencyBuilderForAwsS3 builder = new FluencyBuilderForAwsS3();
            builder.setAwsEndpoint("https://foo.bar.org");
            builder.setAwsRegion("us-east-1");
            builder.setCompressionEnabled(false);
            builder.setSenderAwsAccessKeyId("ACCESSKEYID");
            builder.setSenderAwsSecretAccessKey("SECRETACCESSKEY");
            builder.setBufferChunkInitialSize(2 * 1024 * 1024);
            builder.setBufferChunkRetentionSize(9 * 1024 * 1024);
            builder.setBufferChunkRetentionTimeMillis(1234);
            builder.setMaxBufferSize(42 * 1024 * 1024L);
            builder.setS3KeyPrefix("mydata");
            builder.setS3KeySuffix(".zzz");
            builder.setS3KeyTimeZoneId(ZoneOffset.of("JST", SHORT_IDS));
            builder.setSenderRetryMax(4);
            builder.setSenderMaxRetryIntervalMillis(65432);
            builder.setSenderRetryIntervalMillis(543);
            builder.setSenderRetryFactor(1.234f);
            builder.setSenderWorkBufSize(99 * 1024);
            builderWithCustomConfig = spy(builder);
        }
        doReturn(fluency).when(builderWithCustomConfig)
                .createFluency(
                        any(RecordFormatter.class),
                        any(Ingester.class),
                        any(Buffer.Config.class),
                        any(Flusher.Config.class));
    }

    @ParameterizedTest
    @EnumSource(FluencyBuilderForAwsS3.FormatType.class)
    void buildWithDefaultConfig(FluencyBuilderForAwsS3.FormatType formatType)
    {
        FluencyBuilderForAwsS3 builder = builderWithDefaultConfig;
        AwsS3Sender sender = mock(AwsS3Sender.class);
        doReturn(sender).when(builder).createSender(any(AwsS3Sender.Config.class));

        builder.setFormatType(formatType);
        Fluency fluency = builder.build();
        assertEquals(fluency, this.fluency);

        ArgumentCaptor<AwsS3Sender.Config> configArgumentCaptor = ArgumentCaptor.forClass(AwsS3Sender.Config.class);
        verify(builder, times(1)).createSender(configArgumentCaptor.capture());
        AwsS3Sender.Config senderConfig = configArgumentCaptor.getValue();
        assertNull(senderConfig.getEndpoint());
        assertNull(senderConfig.getRegion());
        assertNull(senderConfig.getAwsAccessKeyId());
        assertNull(senderConfig.getAwsSecretAccessKey());
        assertEquals(10, senderConfig.getRetryMax());
        assertEquals(1000, senderConfig.getRetryIntervalMs());
        assertEquals(30000, senderConfig.getMaxRetryIntervalMs());
        assertEquals(2.0, senderConfig.getRetryFactor());
        assertEquals(8192, senderConfig.getWorkBufSize());
        assertTrue(senderConfig.isCompressionEnabled());

        ArgumentCaptor<RecordFormatter> recordFormatterArgumentCaptor = ArgumentCaptor.forClass(RecordFormatter.class);
        ArgumentCaptor<Ingester> ingesterArgumentCaptor = ArgumentCaptor.forClass(Ingester.class);
        verify(builder, times(1)).buildFromIngester(
                recordFormatterArgumentCaptor.capture(), ingesterArgumentCaptor.capture());

        RecordFormatter recordFormatter = recordFormatterArgumentCaptor.getValue();
        Class<? extends AwsS3RecordFormatter> expectedAwsS3RecordFormatter = null;
        String expectedS3KeySuffix = null;
        switch (formatType) {
            case MESSAGE_PACK:
                expectedAwsS3RecordFormatter = MessagePackRecordFormatter.class;
                expectedS3KeySuffix = ".msgpack.gz";
                break;
            case JSONL:
                expectedAwsS3RecordFormatter = JsonlRecordFormatter.class;
                expectedS3KeySuffix = ".jsonl.gz";
                break;
            case CSV:
                expectedAwsS3RecordFormatter = CsvRecordFormatter.class;
                expectedS3KeySuffix = ".csv.gz";
                break;
        }
        assertEquals(expectedAwsS3RecordFormatter, recordFormatter.getClass());

        AwsS3Ingester ingester = (AwsS3Ingester) ingesterArgumentCaptor.getValue();
        assertEquals(sender, ingester.getSender());
        DefaultS3DestinationDecider destinationDecider =
                (DefaultS3DestinationDecider) ingester.getS3DestinationDecider();
        assertNull(destinationDecider.getKeyPrefix());
        assertEquals(expectedS3KeySuffix, destinationDecider.getKeySuffix());
        assertEquals(UTC, destinationDecider.getZoneId());

        ArgumentCaptor<Buffer.Config> bufferConfigArgumentCaptor = ArgumentCaptor.forClass(Buffer.Config.class);
        ArgumentCaptor<Flusher.Config> flusherConfigArgumentCaptor = ArgumentCaptor.forClass(Flusher.Config.class);
        verify(builder, times(1)).createFluency(
                eq(recordFormatter),
                eq(ingester),
                bufferConfigArgumentCaptor.capture(),
                flusherConfigArgumentCaptor.capture());

        Buffer.Config bufferConfig = bufferConfigArgumentCaptor.getValue();
        assertEquals(512 * 1024 * 1024, bufferConfig.getMaxBufferSize());
        assertEquals(4 * 1024 * 1024, bufferConfig.getChunkInitialSize());
        assertEquals(64 * 1024 * 1024, bufferConfig.getChunkRetentionSize());
        assertEquals(30 * 1000, bufferConfig.getChunkRetentionTimeMillis());
    }

    @Test
    void buildWithCustomConfig()
    {
        FluencyBuilderForAwsS3 builder = builderWithCustomConfig;
        AwsS3Sender sender = mock(AwsS3Sender.class);
        doReturn(sender).when(builder).createSender(any(AwsS3Sender.Config.class));

        builder.setFormatType(FluencyBuilderForAwsS3.FormatType.JSONL);
        Fluency fluency = builder.build();
        assertEquals(fluency, this.fluency);

        ArgumentCaptor<AwsS3Sender.Config> configArgumentCaptor = ArgumentCaptor.forClass(AwsS3Sender.Config.class);
        verify(builder, times(1)).createSender(configArgumentCaptor.capture());
        AwsS3Sender.Config senderConfig = configArgumentCaptor.getValue();
        assertEquals("https://foo.bar.org", senderConfig.getEndpoint());
        assertEquals(Region.US_EAST_1.toString(), senderConfig.getRegion());
        assertEquals("ACCESSKEYID", senderConfig.getAwsAccessKeyId());
        assertEquals("SECRETACCESSKEY", senderConfig.getAwsSecretAccessKey());
        assertEquals(4, senderConfig.getRetryMax());
        assertEquals(543, senderConfig.getRetryIntervalMs());
        assertEquals(65432, senderConfig.getMaxRetryIntervalMs());
        assertEquals(1.234f, senderConfig.getRetryFactor());
        assertEquals(99 * 1024, senderConfig.getWorkBufSize());
        assertFalse(senderConfig.isCompressionEnabled());

        ArgumentCaptor<RecordFormatter> recordFormatterArgumentCaptor = ArgumentCaptor.forClass(RecordFormatter.class);
        ArgumentCaptor<Ingester> ingesterArgumentCaptor = ArgumentCaptor.forClass(Ingester.class);
        verify(builder, times(1)).buildFromIngester(
                recordFormatterArgumentCaptor.capture(), ingesterArgumentCaptor.capture());

        RecordFormatter recordFormatter = recordFormatterArgumentCaptor.getValue();
        assertTrue(recordFormatter instanceof JsonlRecordFormatter);

        AwsS3Ingester ingester = (AwsS3Ingester) ingesterArgumentCaptor.getValue();
        assertEquals(sender, ingester.getSender());
        DefaultS3DestinationDecider destinationDecider =
                (DefaultS3DestinationDecider) ingester.getS3DestinationDecider();
        assertEquals("mydata", destinationDecider.getKeyPrefix());
        assertEquals(".zzz", destinationDecider.getKeySuffix());
        assertEquals(ZoneId.of("JST", SHORT_IDS), destinationDecider.getZoneId());

        ArgumentCaptor<Buffer.Config> bufferConfigArgumentCaptor = ArgumentCaptor.forClass(Buffer.Config.class);
        ArgumentCaptor<Flusher.Config> flusherConfigArgumentCaptor = ArgumentCaptor.forClass(Flusher.Config.class);
        verify(builder, times(1)).createFluency(
                eq(recordFormatter),
                eq(ingester),
                bufferConfigArgumentCaptor.capture(),
                flusherConfigArgumentCaptor.capture());

        Buffer.Config bufferConfig = bufferConfigArgumentCaptor.getValue();
        assertEquals(42 * 1024 * 1024, bufferConfig.getMaxBufferSize());
        assertEquals(2 * 1024 * 1024, bufferConfig.getChunkInitialSize());
        assertEquals(9 * 1024 * 1024, bufferConfig.getChunkRetentionSize());
        assertEquals(1234, bufferConfig.getChunkRetentionTimeMillis());
    }
}
