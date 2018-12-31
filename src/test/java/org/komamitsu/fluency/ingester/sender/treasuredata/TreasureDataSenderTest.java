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

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TreasureDataSenderTest
{
    private final static Charset CHARSET = Charset.forName("UTF-8");
    private final static String DB = "foodb";
    private final static String TABLE = "bartbl";
    private final static String DB_AND_TABLE = "foodb.bartbl";
    private final static byte[] DATA = "hello, world".getBytes(CHARSET);
    private TreasureDataClient client;

    @Before
    public void setUp()
    {
        client = mock(TreasureDataClient.class);
    }

    private void assertImportedFile(File file)
            throws IOException
    {
        try (FileInputStream fileInputStream = new FileInputStream(file);
                GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                DataInputStream dataInputStream = new DataInputStream(gzipInputStream)) {
            byte[] data = new byte[DATA.length];
            dataInputStream.readFully(data);
            assertArrayEquals(DATA, data);
        }
    }

    @Test
    public void send()
            throws IOException
    {
        when(client.importToTable(anyString(), anyString(), anyString(), any(File.class)))
                .thenAnswer(invocation -> {
                    String dummyPath = "/import";
                    assertImportedFile(invocation.getArgument(3));
                    return new Response<>(dummyPath, true, 200, "OK!!!", null);
        });

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(1)).importToTable(
                eq(DB), eq(TABLE), uniqueIdArgumentCaptor.capture(), any(File.class));
        verify(client, times(0)).createDatabase(anyString());
        verify(client, times(0)).createTable(anyString(), anyString());
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }

    @Test
    public void sendWithCreatingTable()
            throws IOException
    {
        AtomicInteger importToTableCalls = new AtomicInteger();
        when(client.importToTable(anyString(), anyString(), anyString(), any(File.class)))
                .thenAnswer(invocation -> {
                    String dummyPath = "/import";
                    if (importToTableCalls.getAndIncrement() == 0) {
                        return new Response<>(dummyPath, false, 404, "Not Found!!!!", null);
                    }
                    assertImportedFile(invocation.getArgument(3));
                    return new Response<>(dummyPath, true, 200, "OK!!!", null);
                });

        when(client.createTable(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String dummyPath = "/createtbl";
                    return new Response<>(dummyPath, true, 200, "OK!!!", null);
                });

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).importToTable(
                eq(DB), eq(TABLE), uniqueIdArgumentCaptor.capture(), any(File.class));
        verify(client, times(0)).createDatabase(anyString());
        verify(client, times(1)).createTable(eq(DB), eq(TABLE));
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }

    @Test
    public void sendWithCreatingDatabase()
            throws IOException
    {
        AtomicInteger importToTableCalls = new AtomicInteger();
        when(client.importToTable(anyString(), anyString(), anyString(), any(File.class)))
                .thenAnswer(invocation -> {
                    String dummyPath = "/import";
                    if (importToTableCalls.getAndIncrement() == 0) {
                        return new Response<>(dummyPath, false, 404, "Not Found!!!!", null);
                    }
                    assertImportedFile(invocation.getArgument(3));
                    return new Response<>(dummyPath, true, 200, "OK!!!", null);
                });

        AtomicInteger createTableCalls = new AtomicInteger();
        when(client.createTable(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String dummyPath = "/createtbl";
                    if (createTableCalls.getAndIncrement() == 0) {
                        return new Response<>(dummyPath, false, 404, "Not Found!!!!", null);
                    }
                    return new Response<>(dummyPath, true, 200, "OK!!!", null);
                });

        when(client.createDatabase(anyString()))
                .thenAnswer(invocation -> {
                    String dummyPath = "/createdb";
                    return new Response<>(dummyPath, true, 200, "OK!!!", null);
                });

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).importToTable(
                eq(DB), eq(TABLE), uniqueIdArgumentCaptor.capture(), any(File.class));
        verify(client, times(1)).createDatabase(eq(DB));
        verify(client, times(2)).createTable(eq(DB), eq(TABLE));
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }
}