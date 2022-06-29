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

package org.komamitsu.fluency.fluentd;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ConfigurableTestServer
{
    private final boolean sslEnabled;

    public ConfigurableTestServer(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
    }

    Exception run(final WithClientSocket withClientSocket, final WithServerPort withServerPort, long timeoutMilli)
            throws Throwable
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        final AtomicReference<ServerSocket> serverSocket = new AtomicReference<ServerSocket>();

        try {
            if (sslEnabled) {
                serverSocket.set(SSLTestSocketFactories.createServerSocket());
            }
            else {
                serverSocket.set(new ServerSocket(0));
            }

            final int serverPort = serverSocket.get().getLocalPort();

            Future<Void> serverSideFuture = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    Socket acceptSocket = serverSocket.get().accept();
                    try {
                        withClientSocket.run(acceptSocket);
                    }
                    finally {
                        acceptSocket.close();
                    }
                    return null;
                }
            });

            Future<Void> testTaskFuture = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    withServerPort.run(serverPort);
                    return null;
                }
            });

            try {
                testTaskFuture.get(timeoutMilli, TimeUnit.MILLISECONDS);
            }
            catch (Exception e) {
                return e;
            }
            finally {
                serverSideFuture.get();
            }
        }
        finally {
            executorService.shutdownNow();
            if (serverSocket.get() != null) {
                serverSocket.get().close();
            }
        }
        return null;
    }

    interface WithClientSocket
    {
        void run(Socket clientSocket)
                throws Exception;
    }

    interface WithServerPort
    {
        void run(int serverPort)
                throws Exception;
    }
}
