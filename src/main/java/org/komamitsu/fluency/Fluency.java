package org.komamitsu.fluency;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.buffer.StreamBuffer;
import org.komamitsu.fluency.buffer.BufferConfig;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.flusher.FlusherConfig;
import org.komamitsu.fluency.flusher.SyncFlusher;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.msgpack.core.MessagePack;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class Fluency
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Fluency.class);
    private final Sender sender;
    private final StreamBuffer buffer;
    private final Flusher flusher;
    private final MessagePack.Config msgpackConfig;
    private final ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    public static Fluency defaultFluency(String host, int port)
            throws IOException
    {
        return new Fluency.Builder(new RetryableSender(new TCPSender(host, port))).build();
    }

    private Fluency(Sender sender, StreamBuffer buffer, Flusher flusher, MessagePack.Config msgpackConfig)
    {
        this.sender = sender;
        this.buffer = buffer;
        this.flusher = flusher;
        this.msgpackConfig = msgpackConfig;
    }

    public void emit(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        outputStream.reset();
        objectMapper.writeValue(outputStream, Arrays.asList(tag, timestamp, data));
        outputStream.close();
        buffer.append(ByteBuffer.wrap(outputStream.toByteArray()));
        flusher.onUpdate(this);
    }

    public void emit(String tag, Map<String, Object> data)
            throws IOException
    {
        emit(tag, System.currentTimeMillis() / 1000, data);
    }

    @Override
    public void flush()
            throws IOException
    {
        buffer.flush(sender);
    }

    @Override
    public void close()
            throws IOException
    {
        flusher.flush(this);
        sender.close();
    }

    public static class Builder
    {
        private final Sender sender;
        private Class<? extends Flusher> flusherClass = SyncFlusher.class;
        private BufferConfig bufferConfig = new BufferConfig.Builder().build();
        private FlusherConfig flusherConfig = new FlusherConfig.Builder().build();
        private MessagePack.Config msgpackConfig;

        public Builder(Sender sender)
        {
            this.sender = sender;
        }

        public Builder setFlusherClass(Class<? extends Flusher> flusherClass)
        {
            this.flusherClass = flusherClass;
            return this;
        }

        public Builder setBufferConfig(BufferConfig bufferConfig)
        {
            this.bufferConfig = bufferConfig;
            return this;
        }

        public Builder setFlusherConfig(FlusherConfig flusherConfig)
        {
            this.flusherConfig = flusherConfig;
            return this;
        }

        public Fluency build()
        {
            StreamBuffer buffer = new StreamBuffer(bufferConfig);
            Exception exception = null;
            Constructor<? extends Flusher> constructor = null;
            try {
                constructor = flusherClass.getConstructor(FlusherConfig.class);
                Flusher flusher = constructor.newInstance(flusherConfig);
                return new Fluency(sender, buffer, flusher, msgpackConfig);
            }
            catch (NoSuchMethodException e) {
                exception = e;
            }
            catch (InvocationTargetException e) {
                exception = e;
            }
            catch (InstantiationException e) {
                exception = e;
            }
            catch (IllegalAccessException e) {
                exception = e;
            }
            throw new IllegalStateException("Failed to build an instance. flusherClass=" + flusherClass, exception);
        }
    }
}
