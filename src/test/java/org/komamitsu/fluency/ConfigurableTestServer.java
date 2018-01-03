package org.komamitsu.fluency;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConfigurableTestServer
{
    interface WithClientSocket
    {
        void run(SocketChannel clientSocketChannel)
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
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        try {
            serverSocketChannel.socket().bind(new InetSocketAddress(0));
            final int serverPort = serverSocketChannel.socket().getLocalPort();

            Future<Void> serverSideFuture = executorService.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    try {
                        withClientSocket.run(socketChannel);
                    }
                    finally {
                        socketChannel.close();
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
            serverSocketChannel.close();
        }
        return null;
    }
}
