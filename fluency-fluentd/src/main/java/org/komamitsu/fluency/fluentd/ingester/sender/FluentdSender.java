/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.fluency.fluentd.ingester.sender;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.ingester.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FluentdSender implements Closeable, Sender {
  private static final Logger LOG = LoggerFactory.getLogger(FluentdSender.class);
  private final Config config;

  protected FluentdSender(Config config) {
    this.config = config;
  }

  protected FluentdSender() {
    this(new FluentdSender.Config());
  }

  public synchronized void send(ByteBuffer buffer) throws IOException {
    sendInternalWithRestoreBufferPositions(Arrays.asList(buffer), null);
  }

  public synchronized void send(List<ByteBuffer> buffers) throws IOException {
    sendInternalWithRestoreBufferPositions(buffers, null);
  }

  public void sendWithAck(List<ByteBuffer> buffers, String ackToken) throws IOException {
    sendInternalWithRestoreBufferPositions(buffers, ackToken);
  }

  private void sendInternalWithRestoreBufferPositions(List<ByteBuffer> buffers, String ackToken)
      throws IOException {
    List<Integer> positions = new ArrayList<>(buffers.size());
    for (ByteBuffer data : buffers) {
      positions.add(data.position());
    }

    try {
      sendInternal(buffers, ackToken);
    } catch (Exception e) {
      for (int i = 0; i < buffers.size(); i++) {
        buffers.get(i).position(positions.get(i));
      }

      ErrorHandler errorHandler = config.getErrorHandler();
      if (errorHandler != null) {
        try {
          errorHandler.handle(e);
        } catch (Exception ex) {
          LOG.warn("Failed to handle an error in the error handler {}", errorHandler, ex);
        }
      }

      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  public abstract boolean isAvailable();

  protected abstract void sendInternal(List<ByteBuffer> buffers, String ackToken)
      throws IOException;

  public static class Config extends Sender.Config {}
}
