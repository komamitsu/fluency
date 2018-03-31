package org.komamitsu.fluency;

import org.komamitsu.fluency.sender.SSLTestServerSocketFactory;

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

    Exception run(final WithClientSocket withClientSocket, final WithServerPort withServerPort, long timeoutMilli)
            throws Throwable
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        final AtomicReference<ServerSocket> serverSocket = new AtomicReference<ServerSocket>();

        try {
            if (sslEnabled) {
                serverSocket.set(new SSLTestServerSocketFactory().create());
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
}
