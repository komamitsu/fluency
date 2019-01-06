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

package org.komamitsu.fluency.treasuredata;

import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.TDHttpClient;
import org.junit.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Optional;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class FluencyBuilderTest
{
    private static final String APIKEY = "12345/1qaz2wsx3edc4rfv5tgb6yhn";

    private void assertDefaultBuffer(Buffer buffer)
    {
        assertThat(buffer, instanceOf(Buffer.class));
        assertThat(buffer.getMaxBufferSize(), is(512 * 1024 * 1024L));
        assertThat(buffer.getFileBackupDir(), is(nullValue()));
        assertThat(buffer.bufferFormatType(), is("packed_forward"));
        assertThat(buffer.getChunkExpandRatio(), is(2f));
        assertThat(buffer.getChunkRetentionSize(), is(4 * 1024 * 1024));
        assertThat(buffer.getChunkInitialSize(), is(1 * 1024 * 1024));
        assertThat(buffer.getChunkRetentionTimeMillis(), is(30000));
        assertThat(buffer.getJvmHeapBufferMode(), is(false));
    }

    private void assertDefaultFlusher(Flusher flusher)
    {
        assertThat(flusher, instanceOf(AsyncFlusher.class));
        AsyncFlusher asyncFlusher = (AsyncFlusher) flusher;
        assertThat(asyncFlusher.isTerminated(), is(false));
        assertThat(asyncFlusher.getFlushIntervalMillis(), is(600));
        assertThat(asyncFlusher.getWaitUntilBufferFlushed(), is(60));
        assertThat(asyncFlusher.getWaitUntilTerminated(), is(60));
    }

    private void assertDefaultFluentdSender(
            TreasureDataSender sender,
            String expectedEndpoint,
            boolean expectedUseSsl,
            String expectedApiKey)
            throws NoSuchFieldException, IllegalAccessException
    {
        assertThat(sender.getRetryInternalMs(), is(1000));
        assertThat(sender.getMaxRetryInternalMs(), is(30000));
        assertThat(sender.getRetryFactor(), is(2.0f));
        assertThat(sender.getRetryMax(), is(10));
        assertThat(sender.getWorkBufSize(), is(8192));

        Field httpClientField = TDClient.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        TDHttpClient tdHttpClient = (TDHttpClient) httpClientField.get(sender.getClient());

        Field configField = TDHttpClient.class.getDeclaredField("config");
        configField.setAccessible(true);
        TDClientConfig config = (TDClientConfig) configField.get(tdHttpClient);

        assertThat(config.endpoint, is(expectedEndpoint));
        assertThat(config.useSSL, is(expectedUseSsl));
        assertThat(config.apiKey.get(), is(expectedApiKey));
    }

    @Test
    public void build()
            throws IOException, NoSuchFieldException, IllegalAccessException
    {
        Fluency fluency = null;
        try {
            fluency = FluencyBuilder.build(APIKEY, new FluencyBuilder.FluencyConfig());
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
                    "api-import.treasuredata.com", true, APIKEY);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void buildWithCustomHttpsEndpoint()
            throws IOException, NoSuchFieldException, IllegalAccessException
    {
        Fluency fluency = null;
        try {
            fluency = FluencyBuilder.build(APIKEY, "https://custom.endpoint.org", new FluencyBuilder.FluencyConfig());
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
                    "custom.endpoint.org", true, APIKEY);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void buildWithCustomHttpsEndpointWithoutScheme()
            throws IOException, NoSuchFieldException, IllegalAccessException
    {
        Fluency fluency = null;
        try {
            fluency = FluencyBuilder.build(APIKEY, "custom.endpoint.org", new FluencyBuilder.FluencyConfig());
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
                    "custom.endpoint.org", true, APIKEY);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void buildWithCustomHttpEndpoint()
            throws IOException, NoSuchFieldException, IllegalAccessException
    {
        Fluency fluency = null;
        try {
            fluency = FluencyBuilder.build(APIKEY, "http://custom.endpoint.org", new FluencyBuilder.FluencyConfig());
            assertDefaultBuffer(fluency.getBuffer());
            assertDefaultFlusher(fluency.getFlusher());
            assertDefaultFluentdSender(
                    (TreasureDataSender) fluency.getFlusher().getIngester().getSender(),
                    "custom.endpoint.org", false, APIKEY);
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }

    @Test
    public void buildWithAllCustomConfig()
            throws IOException
    {
        String tmpdir = System.getProperty("java.io.tmpdir");
        assertThat(tmpdir, is(notNullValue()));

        Fluency fluency = null;
        try {
            FluencyBuilder.FluencyConfig config =
                    new FluencyBuilder.FluencyConfig()
                            .setFlushIntervalMillis(200)
                            .setMaxBufferSize(Long.MAX_VALUE)
                            .setBufferChunkInitialSize(7 * 1024 * 1024)
                            .setBufferChunkRetentionSize(13 * 1024 * 1024)
                            .setBufferChunkRetentionTimeMillis(19 * 1000)
                            .setJvmHeapBufferMode(true)
                            .setWaitUntilBufferFlushed(42)
                            .setWaitUntilFlusherTerminated(24)
                            .setFileBackupDir(tmpdir)
                            .setSenderRetryIntervalMillis(1234)
                            .setSenderMaxRetryIntervalMillis(345678)
                            .setSenderRetryFactor(3.14f)
                            .setSenderRetryMax(17)
                            .setSenderWorkBufSize(123456);

            fluency = FluencyBuilder.build(APIKEY, config);

            assertThat(fluency.getBuffer(), instanceOf(Buffer.class));
            Buffer buffer = fluency.getBuffer();
            assertThat(buffer.getMaxBufferSize(), is(Long.MAX_VALUE));
            assertThat(buffer.getFileBackupDir(), is(tmpdir));
            assertThat(buffer.bufferFormatType(), is("packed_forward"));
            assertThat(buffer.getChunkRetentionTimeMillis(), is(19 * 1000));
            assertThat(buffer.getChunkExpandRatio(), is(2f));
            assertThat(buffer.getChunkInitialSize(), is(7 * 1024 * 1024));
            assertThat(buffer.getChunkRetentionSize(), is(13 * 1024 * 1024));
            assertThat(buffer.getJvmHeapBufferMode(), is(true));

            assertThat(fluency.getFlusher(), instanceOf(AsyncFlusher.class));
            AsyncFlusher flusher = (AsyncFlusher) fluency.getFlusher();
            assertThat(flusher.isTerminated(), is(false));
            assertThat(flusher.getFlushIntervalMillis(), is(200));
            assertThat(flusher.getWaitUntilBufferFlushed(), is(42));
            assertThat(flusher.getWaitUntilTerminated(), is(24));

            assertThat(flusher.getIngester().getSender(), instanceOf(TreasureDataSender.class));
            TreasureDataSender sender = (TreasureDataSender) flusher.getIngester().getSender();
            assertThat(sender.getRetryInternalMs(), is(1234));
            assertThat(sender.getMaxRetryInternalMs(), is(345678));
            assertThat(sender.getRetryFactor(), is(3.14f));
            assertThat(sender.getRetryMax(), is(17));
            assertThat(sender.getWorkBufSize(), is(123456));
        }
        finally {
            if (fluency != null) {
                fluency.close();
            }
        }
    }
}
