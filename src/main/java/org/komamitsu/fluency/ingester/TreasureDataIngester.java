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
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.logging.HttpLoggingInterceptor;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.POST;
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
        private final TreasureDataClient client;

        public TreasureDataSender(String endpoint, String apikey)
        {
            this.client = new TreasureDataClient(endpoint, apikey);
        }

        public void send(String dbAndTableTag, ByteBuffer dataBuffer)
                throws IOException
        {
            String[] dbAndTable = dbAndTableTag.split(".");
            // TODO: Validation
            String database = dbAndTable[0];
            String table = dbAndTable[1];

            File file = File.createTempFile("tmp-fluency-", ".msgpack.gz");
            try {
                try (InputStream in = new ByteBufferBackedInputStream(dataBuffer);
                        OutputStream out = new GZIPOutputStream(
                                Files.newOutputStream(
                                        file.toPath(),
                                        StandardOpenOption.CREATE_NEW,
                                        StandardOpenOption.WRITE))) {

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
                String uniqueId = UUID.randomUUID().toString();
                Failsafe.with(new RetryPolicy().
                        // TODO: Report error to a handler if set & Create database and table if not exists
                        <TreasureDataClient.Response>retryIf(response -> response.getStatusCode() / 100 == 5).
                        withBackoff(1000, 30000, TimeUnit.MILLISECONDS, 2.0).
                        withMaxRetries(12)).
                        run(() -> client.importToTable(database, table, uniqueId, file));
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
            private String endpoint;
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

            @Override
            public TreasureDataSender createInstance()
            {
                return new TreasureDataSender(endpoint, apikey);
            }
        }
    }

    static class TreasureDataClient
    {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        private final String apikey;

        private final Service service;

        interface Service {
            @POST("/v3/table/import_with_id/{database}/{table}/{unique_id}/msgpack.gz")
            @Headers("Content-Type: application/x-msgpack+gzip")
            Call<Void> importToTable(
                    @Header("Authorization") String authHeader,
                    @Path("database") String database,
                    @Path("table") String table,
                    @Path("unique_id") String uniqueId,
                    @Body RequestBody messagePackGzip);
        }

        static class Response<T>
        {
            private final int statusCode;
            private final String message;
            private final T content;

            Response(int statusCode, String message, T content)
            {
                this.statusCode = statusCode;
                this.message = message;
                this.content = content;
            }

            int getStatusCode()
            {
                return statusCode;
            }

            String getMessage()
            {
                return message;
            }

            T getContent()
            {
                return content;
            }
        }

        TreasureDataClient(String endpoint, String apikey)
        {
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
                throws IOException
        {
            String auth = "TD1 " + apikey;
            RequestBody requestBody = RequestBody.create(null, msgpackGzipped);
            retrofit2.Response<Void> response = service.importToTable(auth, database, table, uniqueId, requestBody).execute();
            return new Response<>(response.code(), response.message(), null);
        }
    }
}
