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

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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

    @Test
    public void send()
            throws IOException
    {
        TreasureDataClient client = mock(TreasureDataClient.class);
        when(client.importToTable(anyString(), anyString(), anyString(), any(File.class)))
                .thenAnswer(
                        (Answer<Response<Void>>) invocation -> {
                            File file = invocation.getArgument(3);
                            try (FileInputStream fileInputStream = new FileInputStream(file);
                                    GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                                    DataInputStream dataInputStream = new DataInputStream(gzipInputStream)) {
                                byte[] data = new byte[DATA.length];
                                dataInputStream.readFully(data);
                                assertArrayEquals(DATA, data);
                            }
                            return new Response<>("/abc", true, 200, "OK!!!", null);
                        });

        TreasureDataSender sender = new TreasureDataSender(new TreasureDataSender.Config(), client);
        sender.send(DB_AND_TABLE, ByteBuffer.wrap(DATA));
        ArgumentCaptor<String> uniqueIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(client, times(1)).importToTable(
                eq(DB), eq(TABLE), uniqueIdArgumentCaptor.capture(), any(File.class));
        UUID.fromString(uniqueIdArgumentCaptor.getValue());
    }

    @Test
    public void close()
    {
    }
}