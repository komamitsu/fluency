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

package org.komamitsu.fluency.treasuredata.ingester.sender;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientBuilder;
import com.treasuredata.client.TDClientHttpException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.komamitsu.fluency.NonRetryableException;
import org.komamitsu.fluency.RetryableException;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.DecimalMin;
import org.komamitsu.fluency.validation.annotation.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
    private static final int RETRY_COUNT_FOR_DB_CREATE_DELETE_CONFLICT = 4;
    private static final int RETRY_INTERVAL_MS_FOR_DB_CREATE_DELETE_CONFLICT = 1000;
    private final Config config;
    private final TDClient client;
    private final RetryPolicy retryPolicy;

    public TreasureDataSender()
    {
        this(new Config());
    }

    public TreasureDataSender(Config config)
    {
        config.validateValues();
        this.config = config;
        this.retryPolicy =
                new RetryPolicy().
                        retryOn(ex -> {
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
                                getRetryInternalMs(),
                                getMaxRetryInternalMs(),
                                TimeUnit.MILLISECONDS,
                                getRetryFactor()).
                        withMaxRetries(getRetryMax());

        this.client = buildClient();
    }

    @VisibleForTesting
    protected TDClient buildClient()
    {
        URI uri;
        try {
            uri = new URI(config.getEndpoint());
        }
        catch (URISyntaxException e) {
            throw new NonRetryableException(
                    String.format("Invalid endpoint. %s", config.getEndpoint()), e);
        }

        String host = uri.getHost() != null ? uri.getHost() : config.getEndpoint();

        TDClientBuilder builder = new TDClientBuilder(false)
                .setEndpoint(host)
                .setApiKey(config.getApikey())
                .setRetryLimit(config.getRetryMax())
                .setRetryInitialIntervalMillis(config.getRetryIntervalMs())
                .setRetryMaxIntervalMillis(config.getMaxRetryIntervalMs())
                .setRetryMultiplier(config.getRetryFactor());

        if (uri.getScheme() != null && uri.getScheme().equals("http")) {
            builder.setUseSSL(false);
        }

        if (uri.getPort() > 0) {
            builder.setPort(uri.getPort());
        }

        return builder.build();
    }

    public TDClient getClient()
    {
        return client;
    }

    public int getRetryInternalMs()
    {
        return config.getRetryIntervalMs();
    }

    public int getMaxRetryInternalMs()
    {
        return config.getMaxRetryIntervalMs();
    }

    public float getRetryFactor()
    {
        return config.getRetryFactor();
    }

    public int getRetryMax()
    {
        return config.getRetryMax();
    }

    public int getWorkBufSize()
    {
        return config.getWorkBufSize();
    }

    private void copyStreams(InputStream in, OutputStream out)
            throws IOException
    {
        byte[] buf = new byte[getWorkBufSize()];
        while (true) {
            int readLen = in.read(buf);
            if (readLen < 0) {
                break;
            }
            out.write(buf, 0, readLen);
        }
    }

    private boolean checkDatabaseAndWaitIfNeeded(String database)
    {
        if (client.existsDatabase(database)) {
            return true;
        }

        LOG.warn("The database could be just removed or invisible. Retrying.... database={}", database);

        try {
            TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL_MS_FOR_DB_CREATE_DELETE_CONFLICT);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NonRetryableException(
                    String.format("Failed to create database. database=%s", database), e);
        }

        return false;
    }

    private void createDatabase(String database)
    {
        for (int i = 0; i < RETRY_COUNT_FOR_DB_CREATE_DELETE_CONFLICT; i++) {
            try {
                client.createDatabase(database);
                LOG.info("Created database. database={}", database);
                return;
            }
            catch (TDClientHttpException e) {
                switch (e.getStatusCode()) {
                    case 409:
                        LOG.info("The database already exists. database={}", database);
                        if (checkDatabaseAndWaitIfNeeded(database)) {
                            return;
                        }
                        // Retrying...
                        break;
                    case 401:
                    case 403:
                    case 404:
                        throw new NonRetryableException(
                                String.format("Failed to create database. database=%s", database), e);
                }
            }
            catch (NonRetryableException e) {
                throw e;
            }
            catch (Throwable e) {
                throw new RetryableException(
                        String.format("Failed to create database. database=%s", database), e);
            }
        }

        // Retry over
        throw new NonRetryableException(
                String.format("It seems you don't have a proper permission on the database. database=%s", database));
    }

    private void createTable(String database, String table)
    {
        while (true) {
            try {
                client.createTable(database, table);
                LOG.info("Created table. database={}, table={}", database, table);
                return;
            }
            catch (TDClientHttpException e) {
                switch (e.getStatusCode()) {
                    case 409:
                        LOG.info("The table already exists. database={}, table={}", database, table);
                        return;
                    case 401:
                    case 403:
                        throw new NonRetryableException(
                                String.format("Failed to create table. database=%s, table=%s", database, table), e);
                    case 404:
                        createDatabase(database);
                        // Retry to create the table
                        break;
                    default:
                        throw new RetryableException(
                                String.format("Failed to create table. database=%s, table=%s", database, table), e);
                }
            }
            catch (NonRetryableException e) {
                throw e;
            }
            catch (Throwable e) {
                throw new RetryableException(
                        String.format("Failed to create table. database=%s, table=%s", database, table), e);
            }
        }
    }

    private void importData(String database, String table, String uniqueId, File file)
    {
        LOG.debug("Importing data to TD table: database={}, table={}, uniqueId={}, fileSize={}",
                database, table, uniqueId, file.length());

        while (true) {
            try {
                client.importFile(database, table, file, uniqueId);
                return;
            }
            catch (TDClientHttpNotFoundException e) {
                createTable(database, table);
                // Retry to create the table
            }
            catch (NonRetryableException e) {
                throw e;
            }
            catch (Throwable e) {
                throw new RetryableException(
                        String.format("Failed to import data. database=%s, table=%s", database, table), e);
            }
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
            Failsafe.with(retryPolicy).run(() -> importData(database, table, uniqueId, file));
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
            extends Sender.Config
            implements Validatable
    {
        private String endpoint = "https://api-import.treasuredata.com";
        private String apikey;
        @Min(10)
        private int retryIntervalMs = 1000;
        @Min(10)
        private int maxRetryIntervalMs = 30000;
        @DecimalMin("1.0")
        private float retryFactor = 2;
        @Min(0)
        private int retryMax = 10;
        @Min(1024)
        private int workBufSize = 8192;

        public String getEndpoint()
        {
            return endpoint;
        }

        public void setEndpoint(String endpoint)
        {
            this.endpoint = endpoint;
        }

        public String getApikey()
        {
            return apikey;
        }

        public void setApikey(String apikey)
        {
            this.apikey = apikey;
        }

        public int getRetryIntervalMs()
        {
            return retryIntervalMs;
        }

        public void setRetryIntervalMs(int retryIntervalMs)
        {
            this.retryIntervalMs = retryIntervalMs;
        }

        public int getMaxRetryIntervalMs()
        {
            return maxRetryIntervalMs;
        }

        public void setMaxRetryIntervalMs(int maxRetryIntervalMs)
        {
            this.maxRetryIntervalMs = maxRetryIntervalMs;
        }

        public float getRetryFactor()
        {
            return retryFactor;
        }

        public void setRetryFactor(float retryFactor)
        {
            this.retryFactor = retryFactor;
        }

        public int getRetryMax()
        {
            return retryMax;
        }

        public void setRetryMax(int retryMax)
        {
            this.retryMax = retryMax;
        }

        public int getWorkBufSize()
        {
            return workBufSize;
        }

        public void setWorkBufSize(int workBufSize)
        {
            this.workBufSize = workBufSize;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "endpoint='" + endpoint + '\'' +
                    ", retryIntervalMs=" + retryIntervalMs +
                    ", maxRetryIntervalMs=" + maxRetryIntervalMs +
                    ", retryFactor=" + retryFactor +
                    ", retryMax=" + retryMax +
                    ", workBufSize=" + workBufSize +
                    "} " + super.toString();
        }

        void validateValues()
        {
            validate();
        }
    }
}
