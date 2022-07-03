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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.security.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.msgpack.core.Preconditions.checkNotNull;

public class MockTCPServer
{
    private static final Logger LOG = LoggerFactory.getLogger(MockTCPServer.class);
    private final boolean sslEnabled;
    private final AtomicLong lastEventTimeStampMilli = new AtomicLong();
    private final AtomicInteger threadSeqNum = new AtomicInteger();
    private final ExecutorService executorService;
    private final ServerTask serverTask;
    private final List<Runnable> tasks = new ArrayList<>();

    public MockTCPServer(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        this.executorService = Executors.newCachedThreadPool(r -> new Thread(r, String.format("accepted-socket-worker-%d", threadSeqNum.getAndAdd(1))));
        try {
            serverTask = new ServerTask(executorService, lastEventTimeStampMilli, getEventHandler(),
                    sslEnabled ? SSLTestSocketFactories.createServerSocket() : new ServerSocket(), tasks);
        }
        catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Failed to initialize MockTCPServer", e);
        }
        tasks.add(serverTask);
    }

    protected EventHandler getEventHandler()
    {
        return new EventHandler()
        {
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

    public synchronized void start()
            throws Exception
    {
        executorService.execute(serverTask);

        for (int i = 0; i < 10; i++) {
            int localPort = serverTask.getLocalPort();
            if (localPort > 0) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
        throw new IllegalStateException("Local port open timeout");
    }

    public void waitUntilEventsStop()
            throws InterruptedException
    {
        for (int i = 0; i < 20; i++) {
            if (lastEventTimeStampMilli.get() + 2000 < System.currentTimeMillis()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
        throw new IllegalStateException("Events didn't stop in the expected time");
    }

    @Override
    public String toString()
    {
        return "MockTCPServer{" +
                "serverTask=" + serverTask +
                ", sslEnabled=" + sslEnabled +
                '}';
    }

    public int getLocalPort()
    {
        return serverTask.getLocalPort();
    }

    public synchronized void stop()
            throws IOException
    {
        stop(false);
    }

    public synchronized void stop(boolean immediate)
            throws IOException
    {
        if (executorService == null) {
            return;
        }
        LOG.debug("Stopping MockTCPServer... {}", this);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                LOG.debug("Shutting down MockTCPServer and child tasks... {}", this);
                executorService.shutdownNow();
            }
        }
        catch (InterruptedException e) {
            LOG.warn("ExecutorService.shutdown() was failed: {}", this, e);
            Thread.currentThread().interrupt();
        }

        if (immediate) {
            LOG.debug("Closing related sockets {}", this);
            for (Runnable runnable : tasks) {
                if (runnable instanceof ServerTask) {
                    ((ServerTask) runnable).close();
                } else if (runnable instanceof ServerTask.AcceptTask) {
                    ((ServerTask.AcceptTask) runnable).close();
                }
            }
        }
    }

    public interface EventHandler
    {
        void onConnect(Socket acceptSocket);

        void onReceive(Socket acceptSocket, int len, byte[] data);

        void onClose(Socket acceptSocket);
    }

    private static class ServerTask
            implements Runnable
    {
        private final ServerSocket serverSocket;
        private final ExecutorService serverExecutorService;
        private final EventHandler eventHandler;
        private final AtomicLong lastEventTimeStampMilli;
        private final List<Runnable> tasks;

        private ServerTask(
                ExecutorService executorService,
                AtomicLong lastEventTimeStampMilli,
                EventHandler eventHandler,
                ServerSocket serverSocket,
                List<Runnable> tasks)
                throws IOException
        {
            this.serverExecutorService = checkNotNull(executorService);
            this.lastEventTimeStampMilli = checkNotNull(lastEventTimeStampMilli);
            this.eventHandler = checkNotNull(eventHandler);
            this.serverSocket = checkNotNull(serverSocket);
            if (!serverSocket.isBound()) {
                serverSocket.bind(null);
            }
            this.tasks = checkNotNull(tasks);
        }

        public int getLocalPort()
        {
            return serverSocket.getLocalPort();
        }

        @Override
        public String toString()
        {
            return "ServerTask{" +
                    "serverSocket=" + serverSocket +
                    '}';
        }

        @Override
        public void run()
        {
            while (!serverExecutorService.isShutdown()) {
                try {
                    LOG.debug("ServerTask: accepting... this={}, local.port={}", this, getLocalPort());
                    Socket acceptSocket = serverSocket.accept();
                    LOG.debug("ServerTask: accepted. this={}, local.port={}, remote.port={}", this, getLocalPort(), acceptSocket.getPort());
                    AcceptTask acceptTask = new AcceptTask(serverExecutorService, lastEventTimeStampMilli, eventHandler, acceptSocket);
                    serverExecutorService.execute(acceptTask);
                    tasks.add(acceptTask);
                }
                catch (RejectedExecutionException | ClosedByInterruptException e) {
                    LOG.debug("ServerTask: ServerSocket.accept() failed[{}]: this={}", e.getMessage(), this);
                }
                catch (IOException e) {
                    LOG.warn("ServerTask: ServerSocket.accept() failed[{}]: this={}", e.getMessage(), this);
                }
            }
            try {
                serverSocket.close();
            }
            catch (IOException e) {
                LOG.warn("ServerTask: ServerSocketChannel.close() failed", e);
            }
            LOG.info("ServerTask: Finishing ServerTask...: this={}", this);
        }

        private void close()
                throws IOException
        {
            serverSocket.close();
        }

        private static class AcceptTask
                implements Runnable
        {
            private final Socket acceptSocket;
            private final EventHandler eventHandler;
            private final ExecutorService serverExecutorService;
            private final AtomicLong lastEventTimeStampMilli;

            private AcceptTask(ExecutorService serverExecutorService, AtomicLong lastEventTimeStampMilli, EventHandler eventHandler, Socket acceptSocket)
            {
                this.serverExecutorService = checkNotNull(serverExecutorService);
                this.lastEventTimeStampMilli = checkNotNull(lastEventTimeStampMilli);
                this.eventHandler = checkNotNull(eventHandler);
                this.acceptSocket = checkNotNull(acceptSocket);
            }

            private void close()
                    throws IOException
            {
                acceptSocket.close();
            }

            @Override
            public void run()
            {
                LOG.debug("AcceptTask: connected. this={}, local={}, remote={}",
                        this, acceptSocket.getLocalPort(), acceptSocket.getPort());
                try {
                    eventHandler.onConnect(acceptSocket);
                    byte[] byteBuf = new byte[512 * 1024];
                    while (!serverExecutorService.isShutdown()) {
                        try {
                            int len = acceptSocket.getInputStream().read(byteBuf);
                            if (len <= 0) {
                                LOG.debug("AcceptTask: closed. this={}, local={}, remote={}",
                                        this, acceptSocket.getLocalPort(), acceptSocket.getPort());
                                eventHandler.onClose(acceptSocket);
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
                                lastEventTimeStampMilli.set(System.currentTimeMillis());
                            }
                        }
                        catch (IOException e) {
                            LOG.warn("AcceptTask: recv() failed: this={}, message={}, cause={}",
                                    this, e.getMessage(), e.getCause() == null ? "" : e.getCause().getMessage());
                            if (e instanceof SSLHandshakeException && e.getCause() instanceof EOFException) {
                                eventHandler.onClose(acceptSocket);
                                break;
                            }
                            if (acceptSocket.isClosed()) {
                                eventHandler.onClose(acceptSocket);
                                throw new RuntimeException(e);
                            }
                        }
                    }
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
