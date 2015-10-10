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

public abstract class AbstractMockTCPServer
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractMockTCPServer.class);
    private final ExecutorService executorService;
    private ServerTask serverTask;

    public interface EventHandler
    {
        void onConnect(SocketChannel acceptSocketChannel);

        void onReceive(SocketChannel acceptSocketChannel, ByteBuffer data);

        void onClose(SocketChannel acceptSocketChannel);
    }

    public AbstractMockTCPServer()
            throws IOException
    {
        this.executorService = Executors.newCachedThreadPool();
    }

    protected abstract EventHandler getEventHandler();

    public synchronized void start()
            throws IOException
    {
        if (serverTask == null) {
            serverTask = new ServerTask(executorService, getEventHandler());
            executorService.execute(serverTask);
        }
    }

    public int getLocalPort()
    {
        return serverTask.getLocalPort();
    }

    public void stop()
            throws IOException
    {
        executorService.shutdown();
        try {
            executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("ExecutorService.shutdown() was interrupted", e);
        }
        executorService.shutdownNow();
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
                    LOG.debug("ServerTask: accepting... local.port={}", getLocalPort());
                    SocketChannel accept = serverSocketChannel.accept();
                    LOG.debug("ServerTask: accepted. local.port={}, remote.port={}", getLocalPort(), accept.socket().getPort());
                    serverExecutorService.execute(new AcceptTask(serverExecutorService, eventHandler, accept));
                }
                catch (RejectedExecutionException e) {
                    LOG.debug("ServerSocketChannel.accept() failed", e);
                }
                catch (ClosedByInterruptException e) {
                    LOG.debug("ServerSocketChannel.accept() failed", e);
                }
                catch (IOException e) {
                    LOG.warn("ServerSocketChannel.accept() failed", e);
                }
            }
            try {
                serverSocketChannel.close();
            }
            catch (IOException e) {
                LOG.warn("ServerSocketChannel.close() interrupted", e);
            }
            LOG.info("Finishing ServerTask...");
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
                LOG.debug("AcceptTask: connected. local={}, remote={}", accept.socket().getLocalPort(), accept.socket().getPort());
                try {
                    eventHandler.onConnect(accept);
                    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(512 * 1024);
                    while (accept.isOpen()) {
                        try {
                            byteBuffer.clear();
                            int len = accept.read(byteBuffer);
                            if (len < 0) {
                                LOG.debug("AcceptTask: closed. local={}, remote={}", accept.socket().getLocalPort(), accept.socket().getPort());
                                eventHandler.onClose(accept);
                                try {
                                    accept.close();
                                }
                                catch (IOException e) {
                                    LOG.warn("AcceptTask: close() failed", e);
                                }
                            }
                            else {
                                eventHandler.onReceive(accept, byteBuffer);
                            }
                        }
                        catch (ClosedChannelException e) {
                            LOG.debug("AcceptTask: channel is closed. local={}, remote={}", accept.socket().getLocalPort(), accept.socket().getPort());
                            eventHandler.onClose(accept);
                        }
                        catch (IOException e) {
                            LOG.warn("AcceptTask: recv() failed", e);
                        }
                    }
                }
                finally {
                    try {
                        accept.close();
                    }
                    catch (IOException e) {
                        LOG.warn("AcceptTask: close() failed", e);
                    }
                    parentExecutorService.shutdownNow();
                }
            }
        }
    }
}
