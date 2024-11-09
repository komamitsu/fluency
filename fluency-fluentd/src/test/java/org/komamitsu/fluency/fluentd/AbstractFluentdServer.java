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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFluentdServer extends MockTCPServer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFluentdServer.class);
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private FluentdEventHandler fluentdEventHandler;

  AbstractFluentdServer(boolean sslEnabled) {
    super(sslEnabled);
  }

  @Override
  protected synchronized MockTCPServer.EventHandler getEventHandler() {
    if (this.fluentdEventHandler == null) {
      this.fluentdEventHandler = new FluentdEventHandler(getFluentdEventHandler());
    }
    return fluentdEventHandler;
  }

  protected abstract EventHandler getFluentdEventHandler();

  public interface EventHandler {
    void onConnect(Socket acceptSocket);

    void onReceive(String tag, long timestampMillis, MapValue data);

    void onClose(Socket accpetSocket);
  }

  private static class FluentdEventHandler implements MockTCPServer.EventHandler {
    private static final StringValue KEY_OPTION_SIZE = ValueFactory.newString("size");
    private static final StringValue KEY_OPTION_CHUNK = ValueFactory.newString("chunk");
    private final EventHandler eventHandler;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<Socket, FluentdAcceptTask> fluentdTasks =
        new ConcurrentHashMap<Socket, FluentdAcceptTask>();

    private FluentdEventHandler(EventHandler eventHandler) {
      this.eventHandler = eventHandler;
    }

    private void ack(Socket acceptSocket, String ackResponseToken) throws IOException {
      byte[] ackResponseTokenBytes = ackResponseToken.getBytes(CHARSET);
      ByteBuffer byteBuffer =
          ByteBuffer.allocate(
              1 /* map header */
                  + 1 /* key header */
                  + 3 /* key body */
                  + 2 /* value header(including len) */
                  + ackResponseTokenBytes.length);

      byteBuffer.put((byte) 0x81); /* map header */
      byteBuffer.put((byte) 0xA3); /* key header */
      byteBuffer.put("ack".getBytes(CHARSET)); /* key body */
      byteBuffer.put((byte) 0xD9);
      byteBuffer.put((byte) ackResponseTokenBytes.length);
      byteBuffer.put(ackResponseTokenBytes);
      byteBuffer.flip();
      acceptSocket.getOutputStream().write(byteBuffer.array());
    }

    @Override
    public void onConnect(final Socket acceptSocket) {
      eventHandler.onConnect(acceptSocket);
      try {
        FluentdAcceptTask fluentdAcceptTask = new FluentdAcceptTask(acceptSocket);
        fluentdTasks.put(acceptSocket, fluentdAcceptTask);
        executorService.execute(fluentdAcceptTask);
      } catch (IOException e) {
        fluentdTasks.remove(acceptSocket);
        throw new IllegalStateException("Failed to create FluentdAcceptTask", e);
      }
    }

    @Override
    public void onReceive(Socket acceptSocket, int len, byte[] data) {
      FluentdAcceptTask fluentdAcceptTask = fluentdTasks.get(acceptSocket);
      if (fluentdAcceptTask == null) {
        throw new IllegalStateException("fluentAccept is null: this=" + this);
      }

      LOG.trace(
          "onReceived: local.port={}, remote.port={}, dataLen={}",
          acceptSocket.getLocalPort(),
          acceptSocket.getPort(),
          len);
      try {
        fluentdAcceptTask.getPipedOutputStream().write(data, 0, len);
        fluentdAcceptTask.getPipedOutputStream().flush();
      } catch (IOException e) {
        throw new RuntimeException("Failed to call PipedOutputStream.write(): this=" + this);
      }
    }

    @Override
    public void onClose(Socket acceptSocket) {
      eventHandler.onClose(acceptSocket);
      FluentdAcceptTask fluentdAcceptTask = fluentdTasks.remove(acceptSocket);
      if (fluentdAcceptTask == null) {
        return;
      }
      try {
        if (fluentdAcceptTask.getPipedInputStream() != null) {
          fluentdAcceptTask.getPipedInputStream().close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close PipedInputStream");
      }
      try {
        if (fluentdAcceptTask.getPipedOutputStream() != null) {
          fluentdAcceptTask.getPipedOutputStream().close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close PipedOutputStream");
      }
    }

    private class FluentdAcceptTask implements Runnable {
      private final Socket acceptSocket;
      private final PipedInputStream pipedInputStream;
      private final PipedOutputStream pipedOutputStream;

      private FluentdAcceptTask(Socket acceptSocket) throws IOException {
        this.acceptSocket = acceptSocket;
        this.pipedOutputStream = new PipedOutputStream();
        this.pipedInputStream = new PipedInputStream(pipedOutputStream);
      }

      PipedInputStream getPipedInputStream() {
        return pipedInputStream;
      }

      PipedOutputStream getPipedOutputStream() {
        return pipedOutputStream;
      }

      @Override
      public void run() {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(pipedInputStream);

        try {
          while (!executorService.isTerminated()) {
            ImmutableValue value;
            try {
              if (!unpacker.hasNext()) {
                break;
              }
              value = unpacker.unpackValue();
              LOG.trace(
                  "Received a value: local.port={}, remote.port={}",
                  acceptSocket.getLocalPort(),
                  acceptSocket.getPort());
            } catch (Exception e) {
              LOG.debug("Fluentd accept task received IOException: {}", e.getMessage());
              break;
            }
            assertEquals(ValueType.ARRAY, value.getValueType());
            ImmutableArrayValue rootValue = value.asArrayValue();
            assertEquals(rootValue.size(), 3);

            String tag = rootValue.get(0).toString();
            Value secondValue = rootValue.get(1);

            // PackedForward
            byte[] packedBytes = secondValue.asRawValue().asByteArray();
            MessageUnpacker eventsUnpacker = MessagePack.newDefaultUnpacker(packedBytes);
            while (eventsUnpacker.hasNext()) {
              ImmutableArrayValue arrayValue = eventsUnpacker.unpackValue().asArrayValue();
              assertEquals(2, arrayValue.size());
              Value timestampValue = arrayValue.get(0);
              MapValue mapValue = arrayValue.get(1).asMapValue();
              long timestampMillis;
              if (timestampValue.isIntegerValue()) {
                timestampMillis = timestampValue.asIntegerValue().asLong() * 1000;
              } else if (timestampValue.isExtensionValue()) {
                ExtensionValue extensionValue = timestampValue.asExtensionValue();
                if (extensionValue.getType() != 0) {
                  throw new IllegalArgumentException(
                      "Unexpected extension type: " + extensionValue.getType());
                }
                byte[] data = extensionValue.getData();
                long seconds = ByteBuffer.wrap(data, 0, 4).order(ByteOrder.BIG_ENDIAN).getInt();
                long nanos = ByteBuffer.wrap(data, 4, 4).order(ByteOrder.BIG_ENDIAN).getInt();
                timestampMillis = seconds * 1000 + nanos / 1000000;
              } else {
                throw new IllegalArgumentException("Unexpected value type: " + timestampValue);
              }
              eventHandler.onReceive(tag, timestampMillis, mapValue);
            }

            // Option
            Map<Value, Value> map = rootValue.get(2).asMapValue().map();
            //    "size"
            assertEquals(map.get(KEY_OPTION_SIZE).asIntegerValue().asLong(), packedBytes.length);
            //    "chunk"
            Value chunk = map.get(KEY_OPTION_CHUNK);
            if (chunk != null) {
              ack(acceptSocket, chunk.asStringValue().asString());
            }
          }

          try {
            LOG.debug(
                "Closing unpacker: this={}, local.port={}, remote.port={}",
                this,
                acceptSocket.getLocalPort(),
                acceptSocket.getPort());
            unpacker.close();
          } catch (IOException e) {
            LOG.warn("Failed to close unpacker quietly: this={}, unpacker={}", this, unpacker);
          }
        } catch (Throwable e) {
          LOG.error(
              "Fluentd server failed: this="
                  + this
                  + ", local.port="
                  + acceptSocket.getLocalPort()
                  + ", remote.port="
                  + acceptSocket.getPort(),
              e);
          try {
            acceptSocket.close();
          } catch (IOException e1) {
            LOG.warn("Failed to close accept socket quietly", e1);
          }
        }
      }
    }
  }
}
