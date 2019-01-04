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

import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.NonRetryableException;
import org.komamitsu.fluency.treasuredata.ingester.sender.TreasureDataSender;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TreasureDataSenderTest
{
    private final static Charset CHARSET = Charset.forName("UTF-8");
    private final static String DB = "foodb";
    private final static String TABLE = "bartbl";
    private final static String DB_AND_TABLE = "foodb.bartbl";
    private final static byte[] DATA = "hello, world".getBytes(CHARSET);
    private TDClient client;

    @Before
    public void setUp()
    {
        client = mock(TDClient.class);
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
        doAnswer(invocation -> {
            assertImportedFile(invocation.getArgument(2));
            return null;
        }).when(client).importFile(anyString(), anyString(), any(File.class), anyString());

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(1)).importFile(
                eq(DB), eq(TABLE), any(File.class), uniqueIdArgumentCaptor.capture());
        verify(client, times(0)).createDatabase(anyString());
        verify(client, times(0)).createTable(anyString(), anyString());
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }

    @Test
    public void sendWithCreatingTable()
            throws IOException
    {
        AtomicInteger importToTableCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (importToTableCalls.getAndIncrement() == 0) {
                throw new TDClientHttpNotFoundException("Not Found!!!!");
            }
            assertImportedFile(invocation.getArgument(2));
            return null;
        }).when(client).importFile(anyString(), anyString(), any(File.class), anyString());

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).importFile(
                eq(DB), eq(TABLE), any(File.class), uniqueIdArgumentCaptor.capture());
        verify(client, times(0)).createDatabase(anyString());
        verify(client, times(1)).createTable(eq(DB), eq(TABLE));
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }

    @Test
    public void sendWithCreatingDatabase()
            throws IOException
    {
        AtomicInteger importToTableCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (importToTableCalls.getAndIncrement() == 0) {
                throw new TDClientHttpNotFoundException("Not Found!!!!");
            }
            assertImportedFile(invocation.getArgument(2));
            return null;
        }).when(client).importFile(anyString(), anyString(), any(File.class), anyString());

        AtomicInteger createTableCalls = new AtomicInteger();
        doAnswer(invocation -> {
            if (createTableCalls.getAndIncrement() == 0) {
                throw new TDClientHttpNotFoundException("Not Found!!!!");
            }
            return null;
        }).when(client).createTable(anyString(), anyString());

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(2)).importFile(
                eq(DB), eq(TABLE), any(File.class), uniqueIdArgumentCaptor.capture());
        verify(client, times(1)).createDatabase(eq(DB));
        verify(client, times(2)).createTable(eq(DB), eq(TABLE));
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }

    @Test
    public void sendWithLackOfPermissionOnDatabase()
            throws IOException
    {
        doThrow(new TDClientHttpNotFoundException("Not Found!!!!"))
                .when(client).importFile(anyString(), anyString(), any(File.class), anyString());

        doThrow(new TDClientHttpNotFoundException("Not Found!!!!"))
                .when(client).createTable(anyString(), anyString());

        doThrow(new TDClientHttpConflictException("Conflict!!!!"))
                .when(client).createDatabase(anyString());

        doReturn(false).when(client).existsDatabase(anyString());

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        try {
            sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
            fail();
        }
        catch (NonRetryableException e) {
            assertTrue(true);
        }
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(1)).importFile(
                eq(DB), eq(TABLE), any(File.class), uniqueIdArgumentCaptor.capture());
        verify(client, times(4)).createDatabase(eq(DB));
        verify(client, times(4)).existsDatabase(eq(DB));
        verify(client, times(1)).createTable(eq(DB), eq(TABLE));
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }
}