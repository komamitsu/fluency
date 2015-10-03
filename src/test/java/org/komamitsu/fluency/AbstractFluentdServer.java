package org.komamitsu.fluency;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public abstract class AbstractFluentdServer
        extends AbstractMockTCPServer
{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFluentdServer.class);
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
        private PipedInputStream pipedInputStream;
        private PipedOutputStream pipedOutputStream;

        private FluentdEventHandler(EventHandler eventHandler)
        {
            this.eventHandler = eventHandler;
        }

        @Override
        public void onConnect(SocketChannel acceptSocketChannel)
        {
            pipedOutputStream = new PipedOutputStream();
            try {
                pipedInputStream = new PipedInputStream(pipedOutputStream);
            }
            catch (IOException e) {
                throw new IllegalStateException("Failed to create PipedInputStream");
            }

            eventHandler.onConnect(acceptSocketChannel);
            executorService.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(pipedInputStream);

                    while (!executorService.isTerminated()) {
                        ImmutableValue value = null;
                        try {
                            if (!unpacker.hasNext()) {
                                break;
                            }
                            value = unpacker.unpackValue();
                        }
                        catch (InterruptedIOException e) {
                            // Expected
                            break;
                        }
                        catch (IOException e) {
                            throw new IllegalStateException("Failed to unpack values", e);
                        }
                        ImmutableArrayValue rootValue = value.asArrayValue();

                        String tag = rootValue.get(0).toString();
                        Value secondValue = rootValue.get(1);

                        if (secondValue.isIntegerValue()) {
                            // Message
                            long timestamp = secondValue.asIntegerValue().asLong();
                            MapValue mapValue = rootValue.get(2).asMapValue();
                            try {
                                eventHandler.onReceive(tag, timestamp, mapValue);
                            }
                            catch (AssertionError e) {
                                LOG.warn("AssertionError: tag={}, timestamp={}, mapValue={}", tag, timestamp, mapValue);
                                throw e;
                            }
                        }
                        else if (secondValue.isRawValue()) {
                            // PackedForward
                            byte[] bytes = secondValue.asRawValue().asByteArray();
                            MessageUnpacker eventsUnpacker = MessagePack.newDefaultUnpacker(bytes);
                            try {
                                while (eventsUnpacker.hasNext()) {
                                    ImmutableArrayValue arrayValue = eventsUnpacker.unpackValue().asArrayValue();
                                    assertEquals(2, arrayValue.size());
                                    long timestamp = arrayValue.get(0).asIntegerValue().asLong();
                                    MapValue mapValue = arrayValue.get(1).asMapValue();
                                    eventHandler.onReceive(tag, timestamp, mapValue);
                                }
                            }
                            catch (IOException e) {
                                throw new IllegalStateException("Failed to unpack: unpacker=" + eventsUnpacker, e);
                            }
                        }
                        else {
                            throw new IllegalStateException("Unexpected second value: " + secondValue);
                        }
                    }

                    try {
                        unpacker.close();
                    }
                    catch (IOException e) {
                        LOG.warn("Failed to close unpacker quietly={}", unpacker);
                    }
                }
            });
        }

        @Override
        public void onReceive(SocketChannel acceptSocketChannel, ByteBuffer data)
        {
            if (pipedOutputStream == null) {
                throw new IllegalStateException("pipedOutputStream is null");
            }
            data.flip();
            byte[] bytes = new byte[data.limit()];
            data.get(bytes);
            try {
                pipedOutputStream.write(bytes);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to call PipedOutputStream.write()");
            }
        }

        @Override
        public void onClose(SocketChannel acceptSocketChannel)
        {
            eventHandler.onClose(acceptSocketChannel);
            executorService.shutdown();
            try {
                executorService.awaitTermination(3, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                LOG.warn("onClose() was interrupted", e);
            }
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
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

