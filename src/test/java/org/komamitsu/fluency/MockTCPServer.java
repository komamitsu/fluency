package org.komamitsu.fluency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class MockTCPServer
{
    private static final Logger LOG = LoggerFactory.getLogger(MockTCPServer.class);
    private ExecutorService executorService;
    private ServerTask serverTask;

    public interface EventHandler
    {
        void onConnect(SocketChannel acceptSocketChannel);

        void onReceive(SocketChannel acceptSocketChannel, ByteBuffer data);

        void onClose(SocketChannel acceptSocketChannel);
    }

    public MockTCPServer()
            throws IOException
    {
    }

    protected EventHandler getEventHandler()
    {
        return new EventHandler() {
            @Override
            public void onConnect(SocketChannel acceptSocketChannel)
            {
            }

            @Override
            public void onReceive(SocketChannel acceptSocketChannel, ByteBuffer data)
            {
            }

            @Override
            public void onClose(SocketChannel acceptSocketChannel)
            {
            }
        };
    }

    public synchronized void start()
            throws IOException
    {
        if (executorService == null) {
            this.executorService = Executors.newCachedThreadPool();
        }

        if (serverTask == null) {
            serverTask = new ServerTask(executorService, getEventHandler());
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
        private final ServerSocketChannel serverSocketChannel;
        private final ExecutorService serverExecutorService;
        private final EventHandler eventHandler;

        private ServerTask(ExecutorService executorService, EventHandler eventHandler)
                throws IOException
        {
            this.serverExecutorService = executorService;
            this.eventHandler = eventHandler;
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(null);
        }

        public int getLocalPort()
        {
            return serverSocketChannel.socket().getLocalPort();
        }


        @Override
        public void run()
        {
            while (!serverExecutorService.isShutdown()) {
                try {
                    LOG.debug("ServerTask: accepting... this={}, local.port={}", this, getLocalPort());
                    SocketChannel accept = serverSocketChannel.accept();
                    LOG.debug("ServerTask: accepted. this={}, local.port={}, remote.port={}", this, getLocalPort(), accept.socket().getPort());
                    serverExecutorService.execute(new AcceptTask(serverExecutorService, eventHandler, accept));
                }
                catch (RejectedExecutionException e) {
                    LOG.debug("ServerSocketChannel.accept() failed: this=" + this);
                }
                catch (ClosedByInterruptException e) {
                    LOG.debug("ServerSocketChannel.accept() failed: this=" + this);
                }
                catch (IOException e) {
                    LOG.warn("ServerSocketChannel.accept() failed: this=" + this);
                }
            }
            try {
                serverSocketChannel.close();
            }
            catch (IOException e) {
                LOG.warn("ServerSocketChannel.close() interrupted");
            }
            LOG.info("Finishing ServerTask...: this={}", this);
        }

        private static class AcceptTask
                implements Runnable
        {
            private final ExecutorService parentExecutorService;
            private final SocketChannel accept;
            private final EventHandler eventHandler;

            private AcceptTask(ExecutorService parentExecutorService, EventHandler eventHandler, SocketChannel accept)
            {
                this.parentExecutorService = parentExecutorService;
                this.eventHandler = eventHandler;
                this.accept = accept;
            }

            @Override
            public void run()
            {
                LOG.debug("AcceptTask: connected. this={}, local={}, remote={}", this, accept.socket().getLocalPort(), accept.socket().getPort());
                try {
                    eventHandler.onConnect(accept);
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(512 * 1024);
                    while (accept.isOpen()) {
                        try {
                            byteBuffer.clear();
                            int len = accept.read(byteBuffer);
                            if (len < 0) {
                                LOG.debug("AcceptTask: closed. this={}, local={}, remote={}", this, accept.socket().getLocalPort(), accept.socket().getPort());
                                eventHandler.onClose(accept);
                                try {
                                    accept.close();
                                }
                                catch (IOException e) {
                                    LOG.warn("AcceptTask: close() failed: this=" + this, e);
                                }
                            }
                            else {
                                eventHandler.onReceive(accept, byteBuffer);
                            }
                        }
                        catch (ClosedChannelException e) {
                            LOG.debug("AcceptTask: channel is closed. this={}, local={}, remote={}", this, accept.socket().getLocalPort(), accept.socket().getPort());

                            eventHandler.onClose(accept);
                        }
                        catch (IOException e) {
                            LOG.warn("AcceptTask: recv() failed: this=" + this, e);
                        }
                    }
                }
                finally {
                    try {
                        LOG.debug("AcceptTask: Finished. Closing... this={}, local={}, remote={}", this, accept.socket().getLocalPort(), accept.socket().getPort());
                        accept.close();
                    }
                    catch (IOException e) {
                        LOG.warn("AcceptTask: close() failed", e);
                    }
                }
            }
        }
    }
}
