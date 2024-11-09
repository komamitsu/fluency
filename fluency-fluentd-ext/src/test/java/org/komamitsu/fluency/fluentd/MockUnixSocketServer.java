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

import java.io.Closeable;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockUnixSocketServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MockUnixSocketServer.class);
  private Path socketPath =
      Paths.get(
          System.getProperty("java.io.tmpdir"),
          String.format("fluency-mock-unixsocket-server-%s", UUID.randomUUID()));

  private final AtomicLong lastEventTimeStampMilli = new AtomicLong();
  private final AtomicInteger threadSeqNum = new AtomicInteger();
  private ExecutorService executorService;
  private ServerTask serverTask;
  // TODO Make this class immutable
  private final List<Runnable> tasks = new ArrayList<>();

  private final List<Tuple<MockUnixSocketServer.Type, Integer>> events =
      new CopyOnWriteArrayList<>();

  public List<Tuple<MockUnixSocketServer.Type, Integer>> getEvents() {
    return events;
  }

  @Override
  public void close() throws IOException {
    stop(true);
  }

  public enum Type {
    CONNECT,
    RECEIVE,
    CLOSE;
  }

  protected EventHandler eventHandler() {
    return new EventHandler() {
      @Override
      public void onConnect(SocketChannel acceptSocketChannel) {
        events.add(new Tuple<>(Type.CONNECT, null));
      }

      @Override
      public void onReceive(SocketChannel acceptSocketChannel, ByteBuffer byteBuffer) {
        events.add(new Tuple<>(Type.RECEIVE, byteBuffer.flip().remaining()));
      }

      @Override
      public void onClose(SocketChannel acceptSocketChannel) {
        events.add(new Tuple<>(Type.CLOSE, null));
      }
    };
  }

  public Path getSocketPath() {
    return socketPath;
  }

  public synchronized void start() throws Exception {
    if (executorService == null) {
      this.executorService =
          Executors.newCachedThreadPool(
              r ->
                  new Thread(
                      r, String.format("accepted-socket-worker-%d", threadSeqNum.getAndAdd(1))));
    }

    if (serverTask == null) {
      UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);
      serverTask =
          new ServerTask(
              executorService, lastEventTimeStampMilli, eventHandler(), socketAddress, tasks);
      executorService.execute(serverTask);
      tasks.add(serverTask);
    }
  }

  public void waitUntilEventsStop() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      if (lastEventTimeStampMilli.get() + 2000 < System.currentTimeMillis()) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
    throw new IllegalStateException("Events didn't stop in the expected time");
  }

  public synchronized void stop() throws IOException {
    stop(false);
  }

  public synchronized void stop(boolean immediate) throws IOException {
    LOG.debug("Stopping the mock server... {}", this);
    if (executorService == null) {
      return;
    }
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
        LOG.debug("Shutting down the mock server and child tasks... {}", this);
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
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

    executorService = null;
    serverTask = null;
  }

  private interface EventHandler {
    void onConnect(SocketChannel acceptSocket);

    void onReceive(SocketChannel acceptSocket, ByteBuffer byteBuffer);

    void onClose(SocketChannel acceptSocket);
  }

  private static class ServerTask implements Runnable {
    private final ServerSocketChannel serverSocketChannel;
    private final ExecutorService serverExecutorService;
    private final EventHandler eventHandler;
    private final AtomicLong lastEventTimeStampMilli;
    private final List<Runnable> tasks;
    private final UnixDomainSocketAddress socketAddress;

    @Override
    public String toString() {
      return "ServerTask{"
          + "serverSocketChannel="
          + serverSocketChannel
          + ", lastEventTimeStampMilli="
          + lastEventTimeStampMilli
          + ", socketAddress="
          + socketAddress
          + '}';
    }

    private ServerTask(
        ExecutorService executorService,
        AtomicLong lastEventTimeStampMilli,
        EventHandler eventHandler,
        UnixDomainSocketAddress socketAddress,
        List<Runnable> tasks)
        throws IOException {
      this.serverExecutorService = executorService;
      this.lastEventTimeStampMilli = lastEventTimeStampMilli;
      this.eventHandler = eventHandler;
      this.socketAddress = socketAddress;
      this.serverSocketChannel =
          ServerSocketChannel.open(StandardProtocolFamily.UNIX).bind(socketAddress);
      this.tasks = tasks;
    }

    @Override
    public void run() {
      try {
        while (!serverExecutorService.isShutdown()) {
          try {
            LOG.debug("ServerTask: accepting... this={}", this);
            SocketChannel acceptSocket = serverSocketChannel.accept();
            LOG.debug("ServerTask: accepted. this={}, remote={}", this, acceptSocket);
            AcceptTask acceptTask =
                new AcceptTask(
                    serverExecutorService, lastEventTimeStampMilli, eventHandler, acceptSocket);
            serverExecutorService.execute(acceptTask);
            tasks.add(acceptTask);
          } catch (RejectedExecutionException | ClosedByInterruptException e) {
            LOG.debug(
                "ServerTask: ServerSocketChannel.accept() failed[{}]: this={}",
                e.getMessage(),
                this);
          } catch (IOException e) {
            LOG.warn(
                "ServerTask: ServerSocketChannel.accept() failed[{}]: this={}",
                e.getMessage(),
                this);
          }
        }
      } finally {
        try {
          LOG.debug("ServerTask: closing. this={}", this);
          close();
        } catch (IOException e) {
          LOG.warn("ServerTask: close() failed", e);
        }
      }
      LOG.info("ServerTask: Finishing ServerTask...: this={}", this);
    }

    private void close() throws IOException {
      try {
        serverSocketChannel.close();
      } finally {
        Files.deleteIfExists(socketAddress.getPath());
      }
    }

    private static class AcceptTask implements Runnable {
      private final SocketChannel acceptSocketChannel;
      private final EventHandler eventHandler;
      private final ExecutorService serverExecutorService;
      private final AtomicLong lastEventTimeStampMilli;

      private AcceptTask(
          ExecutorService serverExecutorService,
          AtomicLong lastEventTimeStampMilli,
          EventHandler eventHandler,
          SocketChannel acceptSocketChannel) {
        this.serverExecutorService = serverExecutorService;
        this.lastEventTimeStampMilli = lastEventTimeStampMilli;
        this.eventHandler = eventHandler;
        this.acceptSocketChannel = acceptSocketChannel;
      }

      @Override
      public String toString() {
        return "AcceptTask{"
            + "acceptSocketChannel="
            + acceptSocketChannel
            + ", lastEventTimeStampMilli="
            + lastEventTimeStampMilli
            + '}';
      }

      private void close() throws IOException {
        acceptSocketChannel.close();
      }

      @Override
      public void run() {
        LOG.debug("AcceptTask: connected. this={}", this);
        try {
          eventHandler.onConnect(acceptSocketChannel);
          ByteBuffer byteBuffer = ByteBuffer.allocate(512 * 1024);
          while (!serverExecutorService.isShutdown()) {
            try {
              int len = acceptSocketChannel.read(byteBuffer);
              if (len <= 0) {
                LOG.debug(
                    "AcceptTask: closed. len={}, this={}, remote={}",
                    len,
                    this,
                    acceptSocketChannel);
                eventHandler.onClose(acceptSocketChannel);
                try {
                  close();
                } catch (IOException e) {
                  LOG.warn("AcceptTask: close() failed: this={}", this, e);
                }
                break;
              } else {
                eventHandler.onReceive(acceptSocketChannel, byteBuffer);
                lastEventTimeStampMilli.set(System.currentTimeMillis());
              }
            } catch (IOException e) {
              LOG.warn(
                  "AcceptTask: recv() failed: this={}, message={}, cause={}",
                  this,
                  e.getMessage(),
                  e.getCause() == null ? "" : e.getCause().getMessage());
              if (!acceptSocketChannel.isOpen()) {
                eventHandler.onClose(acceptSocketChannel);
                throw new RuntimeException(e);
              }
            }
          }
        } finally {
          try {
            LOG.debug(
                "AcceptTask: Finished. Closing... this={}, remote={}", this, acceptSocketChannel);
            close();
          } catch (IOException e) {
            LOG.warn("AcceptTask: close() failed", e);
          }
        }
      }
    }
  }
}
