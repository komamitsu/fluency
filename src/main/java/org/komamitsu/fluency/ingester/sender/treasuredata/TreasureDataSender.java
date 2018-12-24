/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.ingester.sender.treasuredata;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.komamitsu.fluency.NonRetryableException;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class TreasureDataSender
        implements Closeable, Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(TreasureDataSender.class);
    private final Config config;
    private final TreasureDataClient client;
    private final RetryPolicy retryPolicy;

    public static class HttpResponseError
        extends RuntimeException
    {
        private final String request;
        private final int statusCode;
        private final String reasonPhrase;

        HttpResponseError(String request, int statusCode, String reasonPhrase)
        {
            super(String.format(
                    "HTTP response error: statusCode=%d, reasonPhrase=%s",
                    statusCode, reasonPhrase));

            this.request = request;
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }

        @Override
        public String toString()
        {
            return "HttpResponseError{" +
                    "request='" + request + '\'' +
                    ", statusCode=" + statusCode +
                    ", reasonPhrase='" + reasonPhrase + '\'' +
                    "} " + super.toString();
        }
    }

    public TreasureDataSender(Config config, TreasureDataClient client)
    {
        this.config = config;
        this.client = client;
        this.retryPolicy =
                new RetryPolicy().
                        <Throwable>retryIf(ex -> {
                            if (ex == null) {
                                // Success. Shouldn't retry.
                                return false;
                            }

                            ErrorHandler errorHandler = config.getErrorHandler();

                            if (errorHandler != null) {
                                errorHandler.handle(ex);
                            }

                            if (ex instanceof InterruptedException || ex instanceof NonRetryableException) {
                                return false;
                            }

                            return true;
                        }).
                        withBackoff(
                                config.retryIntervalMs,
                                config.maxRetryIntervalMs,
                                TimeUnit.MILLISECONDS,
                                config.retryFactor).
                        withMaxRetries(config.retryMax);
    }

    private void copyStreams(InputStream in, OutputStream out)
            throws IOException
    {
        byte[] buf = new byte[config.workBufSize];
        while (true) {
            int readLen = in.read(buf);
            if (readLen < 0) {
                break;
            }
            out.write(buf, 0, readLen);
        }
    }

    public void send(String dbAndTableTag, ByteBuffer dataBuffer)
            throws IOException
    {
        String[] dbAndTable = dbAndTableTag.split("\\.");
        // TODO: Validation
        String database = dbAndTable[0];
        String table = dbAndTable[1];

        File file = File.createTempFile("tmp-fluency-", ".msgpack.gz");
        try {
            try (InputStream in = new ByteBufferBackedInputStream(dataBuffer);
                    OutputStream out = new GZIPOutputStream(
                            Files.newOutputStream(
                                    file.toPath(),
                                    StandardOpenOption.WRITE))) {

                copyStreams(in, out);
            }

            String uniqueId = UUID.randomUUID().toString();
            Failsafe.with(retryPolicy)
                    .run(() -> {
                                LOG.debug("Importing data to TD table: database={}, table={}, uniqueId={}, fileSize={}",
                                        database, table, uniqueId, file.length());

                                Response<Void> response =
                                        client.importToTable(database, table, uniqueId, file);

                                // TODO: Report error to a handler if set & Create database and table if not exists
                                if (!response.isSuccess()) {
                                    throw new HttpResponseError(
                                            response.getRequest(),
                                            response.getStatusCode(),
                                            response.getReasonPhrase());
                                }
                            }
                    );
        }
        finally {
            if (!file.delete()) {
                LOG.warn("Failed to delete a temp file: {}", file.getAbsolutePath());
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public static class Config
            implements Instantiator<TreasureDataSender>
    {
        private Sender.Config baseConfig = new Sender.Config();
        private String endpoint = "https://api-import.treasuredata.com";
        private String apikey;
        private long retryIntervalMs = 1000;
        private long maxRetryIntervalMs = 30000;
        private float retryFactor = 2;
        private int retryMax = 10;
        private int workBufSize = 8192;

        public String getEndpoint()
        {
            return endpoint;
        }

        public Config setEndpoint(String endpoint)
        {
            this.endpoint = endpoint;
            return this;
        }

        public String getApikey()
        {
            return apikey;
        }

        public Config setApikey(String apikey)
        {
            this.apikey = apikey;
            return this;
        }

        public Sender.Config getBaseConfig()
        {
            return baseConfig;
        }

        public Config setBaseConfig(Sender.Config baseConfig)
        {
            this.baseConfig = baseConfig;
            return this;
        }

        public long getRetryIntervalMs()
        {
            return retryIntervalMs;
        }

        public Config setRetryIntervalMs(long retryIntervalMs)
        {
            this.retryIntervalMs = retryIntervalMs;
            return this;
        }

        public long getMaxRetryIntervalMs()
        {
            return maxRetryIntervalMs;
        }

        public Config setMaxRetryIntervalMs(long maxRetryIntervalMs)
        {
            this.maxRetryIntervalMs = maxRetryIntervalMs;
            return this;
        }

        public float getRetryFactor()
        {
            return retryFactor;
        }

        public Config setRetryFactor(float retryFactor)
        {
            this.retryFactor = retryFactor;
            return this;
        }

        public int getRetryMax()
        {
            return retryMax;
        }

        public Config setRetryMax(int retryMax)
        {
            this.retryMax = retryMax;
            return this;
        }

        public int getWorkBufSize()
        {
            return workBufSize;
        }

        public Config setWorkBufSize(int workBufSize)
        {
            this.workBufSize = workBufSize;
            return this;
        }

        public ErrorHandler getErrorHandler()
        {
            return baseConfig.getErrorHandler();
        }

        public Config setErrorHandler(ErrorHandler errorHandler)
        {
            baseConfig.setErrorHandler(errorHandler);
            return this;
        }

        @Override
        public TreasureDataSender createInstance()
        {
            return new TreasureDataSender(this, new TreasureDataClient(endpoint, apikey));
        }
    }
}
