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

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.logging.HttpLoggingInterceptor;
import org.komamitsu.fluency.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;

import java.io.File;
import java.io.IOException;

class TreasureDataClient
{
    private static final Logger LOG = LoggerFactory.getLogger(TreasureDataClient.class);
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

        @POST("/v3/database/create/{database}")
        @Headers("Content-Type: application/json")
        Call<Void> createDatabase(
                @Header("Authorization") String authHeader,
                @Path("database") String database);

        @POST("/v3/table/create/{database}/{table}/log")
        @Headers("Content-Type: application/json")
        Call<Void> createTable(
                @Header("Authorization") String authHeader,
                @Path("database") String database,
                @Path("table") String table);
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

    private String getAuthorizationHeader()
    {
        return "TD1 " + apikey;
    }

    Response<Void> createDatabase(String database)
    {
        retrofit2.Response<Void> response;
        try {
            response = service.createDatabase(getAuthorizationHeader(), database).execute();
        }
        catch (IOException e) {
            throw new RetryableException(
                    String.format("Failed to create the database: %s", database), e);
        }

        return new Response<>(
                String.format("POST %s/v3/database/create/%s", endpoint, database),
                response.isSuccessful(), response.code(), response.message(), null);
    }

    Response<Void> createTable(String database, String table)
    {
        retrofit2.Response<Void> response;
        try {
            response = service.createTable(getAuthorizationHeader(), database, table).execute();
        }
        catch (IOException e) {
            throw new RetryableException(
                    String.format("Failed to create the table: %s.%s", database, table), e);
        }

        return new Response<>(
                String.format("POST %s/v3/table/create/%s/%s/log", endpoint, database, table),
                response.isSuccessful(), response.code(), response.message(), null);
    }

    Response<Void> importToTable(String database, String table, String uniqueId, File msgpackGzipped)
    {
        RequestBody requestBody = RequestBody.create(null, msgpackGzipped);
        retrofit2.Response<Void> response;
        try {
            response = service.importToTable(
                    getAuthorizationHeader(),
                    database, table, uniqueId, requestBody).execute();
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
