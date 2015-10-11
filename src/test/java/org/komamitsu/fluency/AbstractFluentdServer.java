package org.komamitsu.fluency;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.IIOException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractFluentdServer
        extends AbstractMockTCPServer
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFluentdServer.class);
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private FluentdEventHandler fluentdEventHandler;

    public interface EventHandler
    {
        void onConnect(SocketChannel accpetSocketChannel);

        void onReceive(String tag, long timestampMillis, MapValue data);

        void onClose(SocketChannel accpetSocketChannel);
    }

    private static class FluentdEventHandler
            implements AbstractMockTCPServer.EventHandler
    {
        private final EventHandler eventHandler;
        private final ExecutorService executorService = Executors.newCachedThreadPool();
        private final Map<SocketChannel, FluentdAcceptTask> fluentdTasks = new ConcurrentHashMap<SocketChannel, FluentdAcceptTask>();

        private FluentdEventHandler(EventHandler eventHandler)
        {
            this.eventHandler = eventHandler;
        }

        private void ack(SocketChannel acceptSocketChannel, Value option)
                throws IOException
        {
            assertEquals(ValueType.MAP, option.getValueType());
            byte[] bytes = null;
            for (Map.Entry<Value, Value> entry : option.asMapValue().entrySet()) {
                if (entry.getKey().asStringValue().asString().equals("chunk")) {
                    assertEquals(ValueType.BINARY, entry.getValue().getValueType());
                    bytes = entry.getValue().asBinaryValue().asByteArray();
                    break;
                }
            }
            assertNotNull(bytes);
            ByteBuffer byteBuffer = ByteBuffer.allocate(
                    1 /* map header */ +
                    1 /* key header */ +
                    3 /* key body */ +
                    2 /* value header(including len) */ +
                    bytes.length);

            byteBuffer.put((byte) 0x81); /* map header */
            byteBuffer.put((byte) 0xA3); /* key header */
            byteBuffer.put("ack".getBytes(CHARSET));    /* key body */
            byteBuffer.put((byte) 0xC4);
            byteBuffer.put((byte) bytes.length);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            acceptSocketChannel.write(byteBuffer);
        }

        private class FluentdAcceptTask implements Runnable
        {
            private final SocketChannel acceptSocketChannel;
            private final PipedInputStream pipedInputStream;
            private final PipedOutputStream pipedOutputStream;

            private FluentdAcceptTask(SocketChannel acceptSocketChannel)
                    throws IOException
            {
                this.acceptSocketChannel = acceptSocketChannel;
                this.pipedOutputStream = new PipedOutputStream();
                this.pipedInputStream = new PipedInputStream(pipedOutputStream);
            }

            public PipedInputStream getPipedInputStream()
            {
                return pipedInputStream;
            }

            public PipedOutputStream getPipedOutputStream()
            {
                return pipedOutputStream;
            }

            @Override
            public void run()
            {
                MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(pipedInputStream);

                try {
                    while (!executorService.isTerminated()) {
                        ImmutableValue value = null;
                        try {
                            if (!unpacker.hasNext()) {
                                break;
                            }
                            value = unpacker.unpackValue();
                            LOG.trace("value={}, local.port={}, remote.port={}", value, acceptSocketChannel.socket().getLocalPort(), acceptSocketChannel.socket().getPort());
                        }
                        catch (IOException e) {
                            LOG.debug("Fluentd accept task received IOException");
                            break;
                        }
                        assertEquals(ValueType.ARRAY, value.getValueType());
                        ImmutableArrayValue rootValue = value.asArrayValue();

                        String tag = rootValue.get(0).toString();
                        Value secondValue = rootValue.get(1);

                        if (secondValue.isIntegerValue()) {
                            // Message
                            long timestamp = secondValue.asIntegerValue().asLong();
                            MapValue mapValue = rootValue.get(2).asMapValue();
                            eventHandler.onReceive(tag, timestamp, mapValue);
                            if (rootValue.size() == 4) {
                                ack(acceptSocketChannel, rootValue.get(3));
                            }
                        }
                        else if (secondValue.isRawValue()) {
                            // PackedForward
                            byte[] bytes = secondValue.asRawValue().asByteArray();
                            MessageUnpacker eventsUnpacker = MessagePack.newDefaultUnpacker(bytes);
                            while (eventsUnpacker.hasNext()) {
                                ImmutableArrayValue arrayValue = eventsUnpacker.unpackValue().asArrayValue();
                                assertEquals(2, arrayValue.size());
                                long timestamp = arrayValue.get(0).asIntegerValue().asLong();
                                MapValue mapValue = arrayValue.get(1).asMapValue();
                                eventHandler.onReceive(tag, timestamp, mapValue);
                            }
                            if (rootValue.size() == 3) {
                                ack(acceptSocketChannel, rootValue.get(2));
                            }
                        }
                        else {
                            throw new IllegalStateException("Unexpected second value: " + secondValue);
                        }
                    }

                    try {
                        LOG.debug("Closing unpacker: this={}, local.port={}, remote.port={}", this, acceptSocketChannel.socket().getLocalPort(), acceptSocketChannel.socket().getPort());
                        unpacker.close();
                    }
                    catch (IOException e) {
                        LOG.warn("Failed to close unpacker quietly: this={}, unpacker={}", this, unpacker);
                    }
                }
                catch (Throwable e) {
                    LOG.error("Fluentd server failed: this=" + this + ", local.port=" + acceptSocketChannel.socket().getLocalPort() + ", remote.port=" + acceptSocketChannel.socket().getPort(), e);
                    try {
                        acceptSocketChannel.close();
                    }
                    catch (IOException e1) {
                        LOG.warn("Failed to close accept socket quietly", e1);
                    }
                }
            }
        }

        @Override
        public void onConnect(final SocketChannel acceptSocketChannel)
        {
            eventHandler.onConnect(acceptSocketChannel);
            try {
                FluentdAcceptTask fluentdAcceptTask = new FluentdAcceptTask(acceptSocketChannel);
                fluentdTasks.put(acceptSocketChannel, fluentdAcceptTask);
                executorService.execute(fluentdAcceptTask);
            }
            catch (IOException e) {
                fluentdTasks.remove(acceptSocketChannel);
                throw new IllegalStateException("Failed to create FluentdAcceptTask", e);
            }
        }

        @Override
        public void onReceive(SocketChannel acceptSocketChannel, ByteBuffer data)
        {
            FluentdAcceptTask fluentdAcceptTask = fluentdTasks.get(acceptSocketChannel);
            if (fluentdAcceptTask == null) {
                throw new IllegalStateException("fluentAccept is null: this=" + this);
            }
            data.flip();
            byte[] bytes = new byte[data.limit()];
            data.get(bytes);

            LOG.trace("onReceived: local.port={}, remote.port={}, dataLen={}", acceptSocketChannel.socket().getLocalPort(), acceptSocketChannel.socket().getPort(), bytes.length);
            try {
                fluentdAcceptTask.getPipedOutputStream().write(bytes);
                fluentdAcceptTask.getPipedOutputStream().flush();
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to call PipedOutputStream.write(): this=" + this);
            }
        }

        @Override
        public void onClose(SocketChannel acceptSocketChannel)
        {
            eventHandler.onClose(acceptSocketChannel);
            FluentdAcceptTask fluentdAcceptTask = fluentdTasks.remove(acceptSocketChannel);
            try {
                fluentdAcceptTask.getPipedInputStream().close();
            }
            catch (IOException e) {
                LOG.warn("Failed to close PipedInputStream");
            }
            try {
                fluentdAcceptTask.getPipedOutputStream().close();
            }
            catch (IOException e) {
                LOG.warn("Failed to close PipedOutputStream");
            }
        }
    }

    public AbstractFluentdServer()
            throws IOException
    {
        super();
    }

    @Override
    protected synchronized AbstractMockTCPServer.EventHandler getEventHandler()
    {
        if (this.fluentdEventHandler == null) {
            this.fluentdEventHandler = new FluentdEventHandler(getFluentdEventHandler());
        }
        return this.fluentdEventHandler;
    }

    protected abstract EventHandler getFluentdEventHandler();
}

