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

package org.komamitsu.fluency.flusher;

import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.util.ExecutorServiceUtils;
import org.komamitsu.fluency.validation.Validatable;
import org.komamitsu.fluency.validation.annotation.Max;
import org.komamitsu.fluency.validation.annotation.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flusher implements Flushable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Flusher.class);
  private static final int DEFAULT_EVENT_QUEUE_SIZE = 16;
  protected final Buffer buffer;
  protected final Ingester ingester;
  private final AtomicBoolean isTerminated = new AtomicBoolean();
  private final Config config;
  private final BlockingQueue<Boolean> eventQueue =
      new LinkedBlockingQueue<>(DEFAULT_EVENT_QUEUE_SIZE);
  private final ExecutorService executorService =
      ExecutorServiceUtils.newSingleThreadDaemonExecutor();

  public Flusher(Config config, Buffer buffer, Ingester ingester) {
    config.validateValues();
    this.config = config;
    this.buffer = buffer;
    this.ingester = ingester;
    executorService.execute(this::runLoop);
  }

  private void runLoop() {
    Boolean wakeup = null;
    do {
      try {
        wakeup = eventQueue.poll(config.getFlushAttemptIntervalMillis(), TimeUnit.MILLISECONDS);
        boolean force = wakeup != null;
        buffer.flush(ingester, force);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Throwable e) {
        LOG.error("Failed to flush", e);
      }
    } while (!executorService.isShutdown());

    if (wakeup == null) {
      // The above run loop can quit without force buffer flush in the following cases
      // - close() is called right after the repeated non-force buffer flush executed in the run
      // loop
      //
      // In these cases, remaining buffers won't be flushed.
      // So force buffer flush is executed here just in case
      try {
        buffer.flush(ingester, true);
      } catch (Throwable e) {
        LOG.error("Failed to flush", e);
      }
    }
  }

  public Buffer getBuffer() {
    return buffer;
  }

  @Override
  public void flush() {
    eventQueue.offer(true);
  }

  private void flushBufferQuietly() {
    LOG.trace("Flushing the buffer");

    try {
      flush();
    } catch (Throwable e) {
      LOG.error("Failed to call flush()", e);
    }
  }

  private void finishExecutorQuietly() {
    LOG.trace("Finishing the executor");

    ExecutorServiceUtils.finishExecutorService(executorService, config.getWaitUntilBufferFlushed());
  }

  private void closeBufferQuietly() {
    LOG.trace("Closing the buffer");

    try {
      buffer.close();
    } catch (Throwable e) {
      LOG.warn("Failed to close the buffer", e);
    }
  }

  private void closeIngesterQuietly() {
    LOG.trace("Closing the ingester");

    try {
      // Close the socket at the end to prevent the server from failing to read from the connection
      ingester.close();
    } catch (Throwable e) {
      LOG.error("Failed to close the ingester", e);
    }
  }

  private void closeResourcesQuietly() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      Future<Void> future =
          executorService.submit(
              () -> {
                closeBufferQuietly();
                closeIngesterQuietly();
                isTerminated.set(true);
                return null;
              });
      future.get(getWaitUntilTerminated(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.warn("closeBuffer() failed", e);
    } catch (TimeoutException e) {
      LOG.warn("closeBuffer() timed out", e);
    } finally {
      executorService.shutdown();
      ;
    }
  }

  @Override
  public void close() {
    flushBufferQuietly();

    finishExecutorQuietly();

    closeResourcesQuietly();
  }

  public boolean isTerminated() {
    return isTerminated.get();
  }

  public Ingester getIngester() {
    return ingester;
  }

  /** @deprecated As of release 2.4.0, replaced by {@link #getFlushAttemptIntervalMillis()} */
  @Deprecated
  public int getFlushIntervalMillis() {
    return config.getFlushAttemptIntervalMillis();
  }

  public int getFlushAttemptIntervalMillis() {
    return config.getFlushAttemptIntervalMillis();
  }

  public int getWaitUntilBufferFlushed() {
    return config.getWaitUntilBufferFlushed();
  }

  public int getWaitUntilTerminated() {
    return config.getWaitUntilTerminated();
  }

  @Override
  public String toString() {
    return "Flusher{"
        + "isTerminated="
        + isTerminated
        + ", buffer="
        + buffer
        + ", ingester="
        + ingester
        + ", config="
        + config
        + '}';
  }

  public static class Config implements Validatable {
    @Min(20)
    @Max(2000)
    private int flushAttemptIntervalMillis = 600;

    @Min(1)
    private int waitUntilBufferFlushed = 60;

    @Min(1)
    private int waitUntilTerminated = 60;

    /** @deprecated As of release 2.4.0, replaced by {@link #getFlushAttemptIntervalMillis()} */
    @Deprecated
    public int getFlushIntervalMillis() {
      return flushAttemptIntervalMillis;
    }

    /**
     * @deprecated As of release 2.4.0, replaced by {@link #setFlushAttemptIntervalMillis(int
     *     flushAttemptIntervalMillis)}
     */
    @Deprecated
    public void setFlushIntervalMillis(int flushAttemptIntervalMillis) {
      this.flushAttemptIntervalMillis = flushAttemptIntervalMillis;
    }

    public int getFlushAttemptIntervalMillis() {
      return flushAttemptIntervalMillis;
    }

    public void setFlushAttemptIntervalMillis(int flushAttemptIntervalMillis) {
      this.flushAttemptIntervalMillis = flushAttemptIntervalMillis;
    }

    public int getWaitUntilBufferFlushed() {
      return waitUntilBufferFlushed;
    }

    public void setWaitUntilBufferFlushed(int waitUntilBufferFlushed) {
      this.waitUntilBufferFlushed = waitUntilBufferFlushed;
    }

    public int getWaitUntilTerminated() {
      return waitUntilTerminated;
    }

    public void setWaitUntilTerminated(int waitUntilTerminated) {
      this.waitUntilTerminated = waitUntilTerminated;
    }

    void validateValues() {
      validate();
    }

    @Override
    public String toString() {
      return "Config{"
          + "flushAttemptIntervalMillis="
          + flushAttemptIntervalMillis
          + ", waitUntilBufferFlushed="
          + waitUntilBufferFlushed
          + ", waitUntilTerminated="
          + waitUntilTerminated
          + '}';
    }
  }
}
