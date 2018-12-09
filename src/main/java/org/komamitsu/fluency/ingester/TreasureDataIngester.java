/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.ingester;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedRunnable;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.logging.HttpLoggingInterceptor;
import org.komamitsu.fluency.NonRetryableException;
import org.komamitsu.fluency.RetryableException;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.PUT;
import retrofit2.http.Path;

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

public class TreasureDataIngester
        implements Ingester
{
    private static final Logger LOG = LoggerFactory.getLogger(TreasureDataIngester.class);
    private final Config config;
    private final TreasureDataSender sender;

    public TreasureDataIngester(Config config, TreasureDataSender sender)
    {
        this.config = config;
        this.sender = sender;
    }

    @Override
    public void ingest(String tag, ByteBuffer dataBuffer)
            throws IOException
    {
        sender.send(tag, dataBuffer);
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public static class Config
        implements Instantiator<TreasureDataSender>
    {
        @Override
        public Ingester createInstance(TreasureDataSender sender)
        {
            return new TreasureDataIngester(this, sender);
        }
    }

    public static class TreasureDataSender
            implements Closeable, Sender
    {
        private final Config config;
        private final TreasureDataClient client;
        private final RetryPolicy retryPolicy;

        public class HttpResponseError
            extends RuntimeException
        {
            private final String request;
            private final int statusCode;
            private final String reasonPhrase;

            public HttpResponseError(String request, int statusCode, String reasonPhrase)
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

        public TreasureDataSender(Config config)
        {
            this.config = config;
            this.client = new TreasureDataClient(config.getEndpoint(), config.getApikey());
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
                            // TODO: Make these configurable
                            withBackoff(1000, 30000, TimeUnit.MILLISECONDS, 2.0).
                            withMaxRetries(12);
        }

        private void copyStreams(InputStream in, OutputStream out)
                throws IOException
        {
            // TODO: Make it configurable
            byte[] buf = new byte[8192];
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

                                    TreasureDataClient.Response<Void> response =
                                            client.importToTable(database, table, uniqueId, file);

                                    // TODO: Report error to a handler if set & Create database and table if not exists
                                    if (!response.success) {
                                        throw new HttpResponseError(
                                                response.request,
                                                response.statusCode,
                                                response.reasonPhrase);
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
                implements Sender.Instantiator<TreasureDataSender>
        {
            private Sender.Config baseConfig = new Sender.Config();
            private String endpoint = "https://api-import.treasuredata.com";
            private String apikey;

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
                return new TreasureDataSender(this);
            }
        }
    }

    static class TreasureDataClient
    {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final String endpoint;
        private final String apikey;

        private final Service service;

        interface Service {
            @PUT("/v3/table/import_with_id/{database}/{table}/{unique_id}/msgpack.gz")
            @Headers("Content-Type: application/binary")
            Call<Void> importToTable(
                    @Header("Authorization") String authHeader,
                    @Path("database") String database,
                    @Path("table") String table,
                    @Path("unique_id") String uniqueId,
                    @Body RequestBody messagePackGzip);
        }

        static class Response<T>
        {
            private final String request;
            private final boolean success;
            private final int statusCode;
            private final String reasonPhrase;
            private final T content;

            Response(String request, boolean success, int statusCode, String reasonPhrase, T content)
            {
                this.request = request;
                this.success = success;
                this.statusCode = statusCode;
                this.reasonPhrase = reasonPhrase;
                this.content = content;
            }

            public String getRequest()
            {
                return request;
            }

            public boolean isSuccess()
            {
                return success;
            }

            int getStatusCode()
            {
                return statusCode;
            }

            String getReasonPhrase()
            {
                return reasonPhrase;
            }

            T getContent()
            {
                return content;
            }
        }

        TreasureDataClient(String endpoint, String apikey)
        {
            this.endpoint = endpoint;
            this.apikey = apikey;

            HttpLoggingInterceptor logging = new HttpLoggingInterceptor();
            logging.setLevel(HttpLoggingInterceptor.Level.NONE);
            OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
            builder.addInterceptor(logging);
            // TODO: Configure somethings
            OkHttpClient httpClient = builder.build();

            Retrofit retrofit = new Retrofit.Builder().client(httpClient)
                    .baseUrl(endpoint)
                    .addConverterFactory(JacksonConverterFactory.create(OBJECT_MAPPER))
                    .build();

            service = retrofit.create(Service.class);
        }

        Response<Void> importToTable(String database, String table, String uniqueId, File msgpackGzipped)
        {
            String auth = "TD1 " + apikey;
            RequestBody requestBody = RequestBody.create(null, msgpackGzipped);
            retrofit2.Response<Void> response = null;
            try {
                response = service.importToTable(auth, database, table, uniqueId, requestBody).execute();
            }
            catch (IOException e) {
                throw new RetryableException("Failed to import data to TD", e);
            }

            return new Response<>(
                    String.format(
                            "PUT %s/v3/table/import_with_id/%s/%s/%s/msgpack.gz",
                            endpoint, database, table, uniqueId),
                    response.isSuccessful(), response.code(), response.message(), null);
        }
    }
}