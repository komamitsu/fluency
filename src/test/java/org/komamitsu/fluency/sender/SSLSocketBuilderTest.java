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

package org.komamitsu.fluency.sender;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class SSLSocketBuilderTest
{
    private SSLServerSocket serverSocket;

    @Before
    public void setUp()
            throws IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException
    {
        serverSocket = new SSLTestServerSocketFactory().create();
    }

    @After
    public void tearDown()
            throws IOException
    {
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    @Test
    public void testWithServer()
            throws IOException, InterruptedException, ExecutionException, TimeoutException
    {
        final AtomicInteger readLen = new AtomicInteger();
        final byte[] buf = new byte[256];

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Void> future = executorService.submit(new Callable<Void>()
        {
            @Override
            public Void call()
                    throws Exception
            {
                Socket clientSocket = serverSocket.accept();
                readLen.set(clientSocket.getInputStream().read(buf));
                return null;
            }
        });

        SSLSocket sslSocket = new SSLSocketBuilder("localhost", serverSocket.getLocalPort(), 5000, 5000).build();

        try {
            OutputStream outputStream = sslSocket.getOutputStream();
            outputStream.write("hello".getBytes("ASCII"));
            outputStream.flush();
        }
        finally {
            sslSocket.close();
        }

        future.get(10, TimeUnit.SECONDS);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(new String(buf, 0, readLen.get(), "ASCII"), "hello");
    }
}