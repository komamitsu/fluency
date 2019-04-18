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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.aws.s3.ingester.AwsS3Ingester;
import org.komamitsu.fluency.aws.s3.ingester.DefaultS3DestinationDecider;
import org.komamitsu.fluency.aws.s3.ingester.sender.AwsS3Sender;
import org.komamitsu.fluency.aws.s3.recordformat.JsonlRecordFormatter;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.regions.Region;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
        doReturn(fluency).when(builderWithDefaultConfig)
                .buildFromIngester(any(RecordFormatter.class), any(Ingester.class));

        {
            FluencyBuilderForAwsS3 builder = new FluencyBuilderForAwsS3();
            builder.setAwsEndpoint("https://foo.bar.org");
            builder.setAwsRegion(Region.US_EAST_1);
            builder.setCompressionEnabled(false);
            builder.setSenderAwsAccessKeyId("ACCESSKEYID");
            builder.setSenderAwsSecretAccessKey("SECRETACCESSKEY");
            builder.setBufferChunkInitialSize(2 * 1024 * 1024);
            builder.setBufferChunkRetentionSize(9 * 1024 * 1024);
            builder.setBufferChunkRetentionTimeMillis(1234);
            builder.setMaxBufferSize(42 * 1024 * 1024L);
            builder.setS3KeyPrefix("mydata");
            builder.setS3KeySuffix(".zzz");
            builder.setSenderRetryMax(4);
            builder.setSenderMaxRetryIntervalMillis(65432);
            builder.setSenderRetryIntervalMillis(543);
            builder.setSenderRetryFactor(1.234f);
            builderWithCustomConfig = spy(builder);
        }
        doReturn(fluency).when(builderWithCustomConfig)
                .buildFromIngester(any(RecordFormatter.class), any(Ingester.class));
    }

    @Test
    void buildWithDefaultConfig()
    {
        FluencyBuilderForAwsS3 builder = builderWithDefaultConfig;
        AwsS3Sender sender = mock(AwsS3Sender.class);
        doReturn(sender).when(builder).createSender(any(AwsS3Sender.Config.class));

        builder.setFormatType(FluencyBuilderForAwsS3.FormatType.JSONL);
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
        assertTrue(senderConfig.isCompression());

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
        assertNull(destinationDecider.getKeyPrefix());
        assertEquals(".jsonl.gz", destinationDecider.getKeySuffix());
        assertEquals(UTC, destinationDecider.getZoneId());
    }
}