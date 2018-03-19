package org.komamitsu.fluency;

import org.komamitsu.fluency.sender.SSLTestServerSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MockTCPServer
{
    private static final Logger LOG = LoggerFactory.getLogger(MockTCPServer.class);
    private ExecutorService executorService;
    private ServerTask serverTask;
    private final boolean useSsl;

    public MockTCPServer(boolean useSsl)
    {
        this.useSsl = useSsl;
    }

    public interface EventHandler
    {
        void onConnect(Socket acceptSocket);

        void onReceive(Socket acceptSocket, int len, byte[] data);

        void onClose(Socket acceptSocket);
    }

    protected EventHandler getEventHandler()
    {
        return new EventHandler() {
            @Override
            public void onConnect(Socket acceptSocket)
            {

            }

            @Override
            public void onReceive(Socket acceptSocket, int len, byte[] data)
            {

            }

            @Override
            public void onClose(Socket acceptSocket)
            {

            }
        };
    }

    private final AtomicInteger threadSeqNum = new AtomicInteger();

    public synchronized void start()
            throws Exception
    {
        if (executorService == null) {
            this.executorService = Executors.newCachedThreadPool(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r)
                {
                    return new Thread(r, String.format("accepted-socket-worker-%d", threadSeqNum.getAndAdd(1)));
                }
            });
        }

        if (serverTask == null) {
            serverTask = new ServerTask(executorService, getEventHandler(),
                    useSsl ? new SSLTestServerSocketFactory().create() : new ServerSocket());
            executorService.execute(serverTask);
        }
    }

    public int getLocalPort()
    {
        return serverTask.getLocalPort();
    }

    public synchronized void stop()
            throws IOException
    {
        if (executorService == null) {
            return;
        }
        LOG.debug("Stopping MockTCPServer...");
        executorService.shutdown();
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("ExecutorService.shutdown() was interrupted: this=" + this, e);
        }
        executorService.shutdownNow();

        executorService = null;
        serverTask = null;
    }

    private static class ServerTask implements Runnable
    {
        private final ServerSocket serverSocket;
        private final ExecutorService serverExecutorService;
        private final EventHandler eventHandler;

        private ServerTask(ExecutorService executorService, EventHandler eventHandler, ServerSocket serverSocket)
                throws IOException
        {
            this.serverExecutorService = executorService;
            this.eventHandler = eventHandler;
            this.serverSocket = serverSocket;
            if (!serverSocket.isBound())
            serverSocket.bind(null);
        }

        public int getLocalPort()
        {
            return serverSocket.getLocalPort();
        }


        @Override
        public void run()
        {
            while (!serverExecutorService.isShutdown()) {
                try {
                    LOG.debug("ServerTask: accepting... this={}, local.port={}", this, getLocalPort());
                    Socket acceptSocket = serverSocket.accept();
                    LOG.debug("ServerTask: accepted. this={}, local.port={}, remote.port={}", this, getLocalPort(), acceptSocket.getPort());
                    serverExecutorService.execute(new AcceptTask(eventHandler, acceptSocket));
                }
                catch (RejectedExecutionException e) {
                    LOG.debug("ServerSocket.accept() failed[{}]: this={}", e.getMessage(), this);
                }
                catch (ClosedByInterruptException e) {
                    LOG.debug("ServerSocket.accept() failed[{}]: this={}", e.getMessage(), this);
                }
                catch (IOException e) {
                    LOG.warn("ServerSocket.accept() failed[{}]: this={}", e.getMessage(), this);
                }
            }
            try {
                serverSocket.close();
            }
            catch (IOException e) {
                LOG.warn("ServerSocketChannel.close() failed", e);
            }
            LOG.info("Finishing ServerTask...: this={}", this);
        }

        private static class AcceptTask
                implements Runnable
        {
            private final Socket acceptSocket;
            private final EventHandler eventHandler;

            private AcceptTask(EventHandler eventHandler, Socket acceptSocket)
            {
                this.eventHandler = eventHandler;
                this.acceptSocket = acceptSocket;
            }

            @Override
            public void run()
            {
                LOG.debug("AcceptTask: connected. this={}, local={}, remote={}",
                        this, acceptSocket.getLocalPort(), acceptSocket.getPort());
                try {
                    eventHandler.onConnect(acceptSocket);
                    byte[] byteBuf = new byte[512 * 1024];
                    while (true) {
                        try {
                            int len = acceptSocket.getInputStream().read(byteBuf);
                            if (len <= 0) {
                                LOG.debug("AcceptTask: closed. this={}, local={}, remote={}",
                                        this, acceptSocket.getLocalPort(), acceptSocket.getPort());
                                try {
                                    acceptSocket.close();
                                }
                                catch (IOException e) {
                                    LOG.warn("AcceptTask: close() failed: this={}", this, e);
                                }
                                break;
                            }
                            else {
                                eventHandler.onReceive(acceptSocket, len, byteBuf);
                            }
                        }
                        catch (IOException e) {
                            LOG.warn("AcceptTask: recv() failed: this={}, message={}, cause={}",
                                    this, e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage());
                            if (e instanceof SSLHandshakeException && e.getCause() instanceof EOFException) {
                                break;
                            }
                        }
                    }
                    eventHandler.onClose(acceptSocket);
                }
                finally {
                    try {
                        LOG.debug("AcceptTask: Finished. Closing... this={}, local={}, remote={}",
                                this, acceptSocket.getLocalPort(), acceptSocket.getPort());
                        acceptSocket.close();
                    }
                    catch (IOException e) {
                        LOG.warn("AcceptTask: close() failed", e);
                    }
                }
            }
        }
    }
}
